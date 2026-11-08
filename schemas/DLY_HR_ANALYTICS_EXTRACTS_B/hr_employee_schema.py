"""
schemas/DLY_C2T_EXTRACTS_INBOUND_B/hr_employee_schema.py
Data contract enforcing types, precision, and column order for employee extracts.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DecimalType,
    TimestampType
)

# 📤 Contract guaranteed for final outbound text file (employee.dat)
HR_EMPLOYEE_OUTPUT = StructType([
    StructField("EMPLOYEE_ID", LongType(), nullable=False),
    StructField("FULL_NAME", StringType(), nullable=True),
    StructField("EMAIL", StringType(), nullable=True),
    StructField("PHONE_NUMBER", StringType(), nullable=True),
    StructField("HIRE_DATE", StringType(), nullable=True),
    StructField("JOB_ID", StringType(), nullable=True),
    StructField("SALARY", DecimalType(10, 2), nullable=True),
    StructField("DEPARTMENT_NAME", StringType(), nullable=True),
    StructField("EXTRACT_TIMESTAMP", TimestampType(), nullable=False)
])