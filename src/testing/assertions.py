from typing import Dict, List, Any, Optional, Union, Callable
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from pyspark.sql import functions as F
import logging

logger = logging.getLogger("testing.assertions")

class DataFrameAssertions:
    """Comprehensive assertions for PySpark DataFrames."""
    
    @staticmethod
    def assert_schema_equal(actual: DataFrame, expected: StructType, 
                          ignore_nullable: bool = False) -> None:
        """Assert that the DataFrame schema matches the expected schema."""
        actual_schema = actual.schema
        
        if ignore_nullable:
            # Compare only field names and types, not nullability
            actual_fields = [(field.name, field.dataType) for field in actual_schema.fields]
            expected_fields = [(field.name, field.dataType) for field in expected.fields]
            
            if actual_fields != expected_fields:
                raise AssertionError(f"Schema mismatch (ignoring nullability):\n" +
                                    f"Actual: {actual_fields}\n" +
                                    f"Expected: {expected_fields}")
        else:
            if actual_schema != expected:
                raise AssertionError(f"Schema mismatch:\n" +
                                    f"Actual: {actual_schema.simpleString()}\n" +
                                    f"Expected: {expected.simpleString()}")
    
    @staticmethod
    def assert_column_exists(df: DataFrame, column_name: str) -> None:
        """Assert that the DataFrame has the specified column."""
        if column_name not in df.columns:
            raise AssertionError(f"Column '{column_name}' not found in DataFrame columns: {df.columns}")
    
    @staticmethod
    def assert_columns_exist(df: DataFrame, column_names: List[str]) -> None:
        """Assert that the DataFrame has all the specified columns."""
        missing_columns = [col for col in column_names if col not in df.columns]
        if missing_columns:
            raise AssertionError(f"Columns {missing_columns} not found in DataFrame columns: {df.columns}")
    
    @staticmethod
    def assert_row_count(df: DataFrame, expected_count: int) -> None:
        """Assert that the DataFrame has the expected number of rows."""
        actual_count = df.count()
        if actual_count != expected_count:
            raise AssertionError(f"Row count mismatch: Actual={actual_count}, Expected={expected_count}")
    
    @staticmethod
    def assert_row_count_min(df: DataFrame, min_count: int) -> None:
        """Assert that the DataFrame has at least the specified number of rows."""
        actual_count = df.count()
        if actual_count < min_count:
            raise AssertionError(f"Row count too low: Actual={actual_count}, Expected at least={min_count}")
    
    @staticmethod
    def assert_row_count_max(df: DataFrame, max_count: int) -> None:
        """Assert that the DataFrame has at most the specified number of rows."""
        actual_count = df.count()
        if actual_count > max_count:
            raise AssertionError(f"Row count too high: Actual={actual_count}, Expected at most={max_count}")
    
    @staticmethod
    def assert_column_values_in_set(df: DataFrame, column_name: str, 
                                  valid_values: List[Any]) -> None:
        """Assert that all values in the column are in the specified set."""
        # First check if column exists
        DataFrameAssertions.assert_column_exists(df, column_name)
        
        # Count values not in the valid set
        invalid_count = df.filter(~F.col(column_name).isin(valid_values)).count()
        
        if invalid_count > 0:
            # Get some examples of invalid values
            invalid_examples = df.filter(~F.col(column_name).isin(valid_values)) \
                                .select(column_name) \
                                .distinct() \
                                .limit(5) \
                                .collect()
            
            invalid_values = [row[column_name] for row in invalid_examples]
            
            raise AssertionError(f"Found {invalid_count} rows with invalid values in column '{column_name}'. " +
                                f"Examples of invalid values: {invalid_values}. " +
                                f"Valid values are: {valid_values}")
    
    @staticmethod
    def assert_column_values_not_null(df: DataFrame, column_name: str) -> None:
        """Assert that there are no null values in the column."""
        # First check if column exists
        DataFrameAssertions.assert_column_exists(df, column_name)
        
        # Count null values
        null_count = df.filter(F.col(column_name).isNull()).count()
        
        if null_count > 0:
            raise AssertionError(f"Found {null_count} null values in column '{column_name}'")
    
    @staticmethod
    def assert_column_values_match_regex(df: DataFrame, column_name: str, 
                                      regex: str) -> None:
        """Assert that all values in the column match the regex pattern."""
        # First check if column exists
        DataFrameAssertions.assert_column_exists(df, column_name)
        
        # Count values not matching the regex
        non_matching_count = df.filter(~F.col(column_name).rlike(regex)).count()
        
        if non_matching_count > 0:
            # Get some examples of non-matching values
            non_matching_examples = df.filter(~F.col(column_name).rlike(regex)) \
                                    .select(column_name) \
                                    .distinct() \
                                    .limit(5) \
                                    .collect()
            
            non_matching_values = [row[column_name] for row in non_matching_examples]
            
            raise AssertionError(f"Found {non_matching_count} rows with values not matching regex '{regex}' " +
                                f"in column '{column_name}'. " +
                                f"Examples of non-matching values: {non_matching_values}")
    
    @staticmethod
    def assert_column_values_in_range(df: DataFrame, column_name: str, 
                                    min_value: Any, max_value: Any) -> None:
        """Assert that all values in the column are within the specified range."""
        # First check if column exists
        DataFrameAssertions.assert_column_exists(df, column_name)
        
        # Count values outside the range
        out_of_range_count = df.filter(
            (F.col(column_name) < min_value) | (F.col(column_name) > max_value)
        ).count()
        
        if out_of_range_count > 0:
            # Get some examples of out-of-range values
            out_of_range_examples = df.filter(
                (F.col(column_name) < min_value) | (F.col(column_name) > max_value)
            ).select(column_name) \
             .distinct() \
             .limit(5) \
             .collect()
            
            out_of_range_values = [row[column_name] for row in out_of_range_examples]
            
            raise AssertionError(f"Found {out_of_range_count} rows with values outside range [{min_value}, {max_value}] " +
                                f"in column '{column_name}'. " +
                                f"Examples of out-of-range values: {out_of_range_values}")
    
    @staticmethod
    def assert_column_values_unique(df: DataFrame, column_name: str) -> None:
        """Assert that all values in the column are unique."""
        # First check if column exists
        DataFrameAssertions.assert_column_exists(df, column_name)
        
        # Count distinct values
        total_count = df.count()
        distinct_count = df.select(column_name).distinct().count()
        
        if total_count != distinct_count:
            # Get some examples of duplicate values
            duplicate_examples = df.groupBy(column_name) \
                                  .count() \
                                  .filter(F.col("count") > 1) \
                                  .select(column_name, "count") \
                                  .orderBy(F.col("count").desc()) \
                                  .limit(5) \
                                  .collect()
            
            duplicates = [(row[column_name], row["count"]) for row in duplicate_examples]
            
            raise AssertionError(f"Found {total_count - distinct_count} duplicate values in column '{column_name}'. " +
                                f"Examples of duplicates (value, count): {duplicates}")
    
    @staticmethod
    def assert_dataframes_equal(actual: DataFrame, expected: DataFrame, 
                              ignore_nullable: bool = False,
                              ignore_row_order: bool = True) -> None:
        """Assert that two DataFrames are equal in schema and data."""
        # Check schema equality
        DataFrameAssertions.assert_schema_equal(actual, expected.schema, ignore_nullable)
        
        # Check row count equality
        actual_count = actual.count()
        expected_count = expected.count()
        
        if actual_count != expected_count:
            raise AssertionError(f"Row count mismatch: Actual={actual_count}, Expected={expected_count}")
        
        # Check data equality
        if ignore_row_order:
            # Sort both DataFrames by all columns to ignore row order
            sorted_columns = actual.columns
            sorted_actual = actual.orderBy(*sorted_columns)
            sorted_expected = expected.orderBy(*sorted_columns)
            
            # Check if the DataFrames are equal
            diff1 = sorted_actual.exceptAll(sorted_expected)
            diff2 = sorted_expected.exceptAll(sorted_actual)
        else:
            # Row order matters, so don't sort
            diff1 = actual.exceptAll(expected)
            diff2 = expected.exceptAll(actual)
        
        diff1_count = diff1.count()
        diff2_count = diff2.count()
        
        if diff1_count > 0 or diff2_count > 0:
            diff1_sample = diff1.limit(5).collect()
            diff2_sample = diff2.limit(5).collect()
            
            raise AssertionError(
                f"Data mismatch: {diff1_count + diff2_count} rows differ.\n" +
                f"Rows in actual but not in expected ({diff1_count} total): {diff1_sample}\n" +
                f"Rows in expected but not in actual ({diff2_count} total): {diff2_sample}"
            )