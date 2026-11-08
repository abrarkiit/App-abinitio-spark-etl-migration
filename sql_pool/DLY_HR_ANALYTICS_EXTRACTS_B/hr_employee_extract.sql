-- Pushdown extract query executed directly against Oracle HR Schema
SELECT 
    e.EMPLOYEE_ID,
    e.FIRST_NAME,
    e.LAST_NAME,
    e.EMAIL,
    e.PHONE_NUMBER,
    TO_CHAR(e.HIRE_DATE, 'YYYY-MM-DD') AS HIRE_DATE,
    e.JOB_ID,
    e.SALARY,
    COALESCE(e.COMMISSION_PCT, 0.00)   AS COMMISSION_PCT,
    e.MANAGER_ID,
    e.DEPARTMENT_ID
FROM HR.EMPLOYEES e
WHERE e.SALARY > 0