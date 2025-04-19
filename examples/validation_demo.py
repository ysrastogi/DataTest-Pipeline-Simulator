from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

from src.simulator.mock_data import MockDataGenerator
from src.validation.core import ValidationEngine
from src.validation.schema.validators import SchemaValidator
from src.validation.quality.checks import ColumnNullCheck, UniqueValueCheck, PatternCheck
from src.validation.quality.profiles import DataQualityProfile, DataQualitySuite
from src.validation.rules.business_rules import ColumnValueRule, CrossColumnRule
from src.validation.rules.rule_engine import RuleEngine
from src.validation.anomaly.detectors import StatisticalAnomalyDetector, IQRAnomalyDetector

# Initialize Spark
spark = SparkSession.builder.appName("ValidationDemo").getOrCreate()

# Generate mock data
print("Generating mock data...")
data_generator = MockDataGenerator(spark)
user_df = data_generator.generate_user_data(count=500)
user_df.printSchema()
user_df.show(5)

# Create validation engine
print("\nCreating validation engine...")
validation_engine = ValidationEngine()

# 1. Add schema validation
print("Adding schema validators...")
expected_schema = StructType([
    StructField("user_id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("country", StringType(), True),
    StructField("registration_date", StringType(), True)
])

schema_validator = SchemaValidator(
    expected_schema=expected_schema, 
    name="UserSchemaValidator",
    strict=False
)
validation_engine.add_validator(schema_validator)

# 2. Add data quality checks
print("Adding data quality validators...")

# Create a data quality suite
quality_suite = DataQualitySuite("UserDataQualitySuite")
quality_suite.add_check(ColumnNullCheck(columns=["user_id", "email"], threshold=0.0))
quality_suite.add_check(UniqueValueCheck(columns=["user_id", "email"]))
quality_suite.add_check(PatternCheck(
    column="email", 
    pattern=r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
))

validation_engine.add_validator(quality_suite)

# 3. Add business rule validation
print("Adding business rule validators...")

# Create a rule engine
rule_engine = RuleEngine("UserBusinessRules")
rule_engine.add_rule(ColumnValueRule(column="age", condition="age >= 18"))
rule_engine.add_rule(CrossColumnRule(condition="country IS NOT NULL OR age > 21"))

validation_engine.add_validator(rule_engine)

# 4. Add anomaly detection
print("Adding anomaly detectors...")
validation_engine.add_validator(StatisticalAnomalyDetector(column="age", threshold=2.5))
validation_engine.add_validator(IQRAnomalyDetector(column="age", iqr_multiplier=1.5))

# 5. Create data quality profile
validation_engine.add_validator(DataQualityProfile())

# Run validation
print("\nRunning validation...")
validation_results = validation_engine.validate(user_df)

# Print validation results
print("\nValidation Results:")
for validator_name, result in validation_results.items():
    print(f"\n{validator_name}: {'✅ PASSED' if result.success else '❌ FAILED'}")
    
    # Print specific details based on validator type
    if "Schema" in validator_name:
        if not result.success and "issues" in result.details:
            print("  Schema issues:")
            for issue in result.details["issues"]:
                print(f"  - {issue}")
    
    elif "Quality" in validator_name:
        if "check_results" in result.details:
            checks_passed = result.details.get("checks_passed", 0)
            checks_total = result.details.get("check_count", 0)
            print(f"  Checks passed: {checks_passed}/{checks_total}")
            
            # Show failed checks
            for check_name, check_result in result.details.get("check_results", {}).items():
                if not check_result.get("success", True):
                    print(f"  - ❌ {check_name} failed")
    
    elif "Business" in validator_name:
        if "rule_results" in result.details:
            rules_passed = result.details.get("passed_rules", 0)
            rules_total = result.details.get("total_rules", 0)
            violations = result.details.get("total_violations", 0)
            print(f"  Rules passed: {rules_passed}/{rules_total}")
            print(f"  Total violations: {violations}")
            
            # Show violations by rule
            for rule_name, violation_count in result.details.get("violations_by_rule", {}).items():
                if violation_count > 0:
                    print(f"  - Rule '{rule_name}': {violation_count} violations")
    
    elif "Anomaly" in validator_name:
        if "anomaly_count" in result.details:
            anomaly_count = result.details.get("anomaly_count", 0)
            total_records = result.details.get("total_records", 0)
            print(f"  Anomalies: {anomaly_count}/{total_records} records ({result.details.get('anomaly_percentage', 0)*100:.2f}%)")
            
            # Show sample anomalies
            if "sample_anomalies" in result.details and result.details["sample_anomalies"]:
                print("  Sample anomalies:")
                for i, anomaly in enumerate(result.details["sample_anomalies"][:3]):
                    print(f"  - {anomaly}")
    
    elif "Profile" in validator_name:
        if "basic_stats" in result.details:
            stats = result.details["basic_stats"]
            print(f"  Records: {stats.get('row_count')}")
            print(f"  Columns: {stats.get('column_count')}")

print("\nValidation complete!")