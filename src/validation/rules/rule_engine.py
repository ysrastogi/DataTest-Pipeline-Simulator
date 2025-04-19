from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from typing import Dict, List, Any, Union

from src.validation.core import Validator, ValidationResult
from src.validation.rules.business_rules import BusinessRule

class RuleEngine(Validator):
    """
    Engine to apply business rules to data.
    """
    
    def __init__(self, name: str = "RuleEngine", rules: List[BusinessRule] = None):
        """
        Initialize the rule engine.
        
        Args:
            name: Name of this engine
            rules: List of BusinessRule objects
        """
        super().__init__(name)
        self.rules = rules or []
    
    def add_rule(self, rule: BusinessRule) -> None:
        """Add a rule to the engine."""
        self.rules.append(rule)
        self.logger.info(f"Added rule: {rule.name}")
    
    def validate(self, df: DataFrame, **kwargs) -> ValidationResult:
        """
        Apply all rules and collect violations.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            ValidationResult summarizing the results
        """
        if not self.rules:
            return ValidationResult(
                success=True,
                name=self.name,
                details={"message": "No rules defined"}
            )
        
        # Apply each rule and collect violations
        all_violations = []
        rule_results = {}
        
        for rule in self.rules:
            try:
                violations = rule.apply(df)
                violation_count = violations.count()
                
                rule_results[rule.name] = {
                    "violation_count": violation_count,
                    "passed": violation_count == 0
                }
                
                if violation_count > 0:
                    # Store violations for reporting
                    violations_data = violations.limit(100).collect()  # Limit to avoid OOM
                    rule_results[rule.name]["sample_violations"] = [row.asDict() for row in violations_data]
                    
                    # Add to overall violations
                    all_violations.append(violations)
            
            except Exception as e:
                self.logger.error(f"Error applying rule {rule.name}: {e}", exc_info=True)
                rule_results[rule.name] = {
                    "error": str(e),
                    "passed": False
                }
        
        # Combine all violations if any
        total_violations = 0
        violations_by_rule = {}
        
        for rule_name, result in rule_results.items():
            if "violation_count" in result:
                violation_count = result["violation_count"]
                total_violations += violation_count
                violations_by_rule[rule_name] = violation_count
        
        # Success if no violations
        success = total_violations == 0
        
        details = {
            "total_rules": len(self.rules),
            "passed_rules": sum(1 for r in rule_results.values() if r.get("passed", False)),
            "failed_rules": sum(1 for r in rule_results.values() if not r.get("passed", False)),
            "total_violations": total_violations,
            "violations_by_rule": violations_by_rule,
            "rule_results": rule_results
        }
        
        return ValidationResult(success=success, name=self.name, details=details)
    
    def get_violations(self, df: DataFrame) -> DataFrame:
        """
        Get a DataFrame containing all violations.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            DataFrame with all violations
        """
        if not self.rules:
            # Return empty DataFrame with base schema plus violation columns
            schema = df.schema.add("rule_name", StringType(), False) \
                             .add("violation_reason", StringType(), False)
            return df.sparkSession.createDataFrame([], schema)
        
        # Apply each rule and union the results
        violations_dfs = []
        
        for rule in self.rules:
            try:
                violations = rule.apply(df)
                if violations.count() > 0:
                    violations_dfs.append(violations)
            except Exception as e:
                self.logger.error(f"Error getting violations for rule {rule.name}: {e}", exc_info=True)
        
        if not violations_dfs:
            # No violations found, return empty DataFrame
            schema = df.schema.add("rule_name", StringType(), False) \
                             .add("violation_reason", StringType(), False)
            return df.sparkSession.createDataFrame([], schema)
        
        # Union all violation DataFrames
        from functools import reduce
        from pyspark.sql import DataFrame
        
        return reduce(DataFrame.unionAll, violations_dfs)