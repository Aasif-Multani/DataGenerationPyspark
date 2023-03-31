Open SQL*Plus or any other SQL editor that you prefer and connect to your Oracle database using your username and password.

Create a new table by using the CREATE TABLE statement followed by the table name and column definitions. In this example, we'll create a table called "employees" that will contain information about company employees:

CREATE TABLE employees (
  employee_id   NUMBER(6),
  first_name    VARCHAR2(20),
  last_name     VARCHAR2(25),
  hire_date     DATE,
  salary        NUMBER(8,2),
  CONSTRAINT emp_pk PRIMARY KEY (employee_id)
);

Next, create a new package specification by using the CREATE PACKAGE statement followed by the package name. In this example, we'll create a package called "employee_pkg" that will contain a procedure for inserting a new employee record into the "employees" table:

CREATE OR REPLACE PACKAGE employee_pkg AS
PROCEDURE insert_employee(
  p_emp_id   NUMBER,
  p_first_name VARCHAR2,
  p_last_name VARCHAR2,
  p_hire_date DATE,
  p_salary NUMBER);
END employee_pkg;

Next, create the package body by using the CREATE PACKAGE BODY statement followed by the package name. In this example, we'll create the implementation of the "insert_employee" procedure:

CREATE OR REPLACE PACKAGE BODY employee_pkg AS
PROCEDURE insert_employee(
  p_emp_id   NUMBER,
  p_first_name VARCHAR2,
  p_last_name VARCHAR2,
  p_hire_date DATE,
  p_salary NUMBER) IS
BEGIN
  INSERT INTO employees
  (employee_id, first_name, last_name, hire_date, salary)
  VALUES
  (p_emp_id, p_first_name, p_last_name, p_hire_date, p_salary);

  COMMIT;
END insert_employee;
END employee_pkg;

Save the table and package specification and package body. You can now execute the "insert_employee" procedure defined in the package by calling it from other SQL or PL/SQL code.

For example:

DECLARE
  v_emp_id   NUMBER := 1001;
  v_first_name VARCHAR2(20) := 'John';
  v_last_name VARCHAR2(25) := 'Doe';
  v_hire_date DATE := SYSDATE;
  v_salary NUMBER := 50000;
BEGIN
  employee_pkg.insert_employee(
    p_emp_id => v_emp_id,
    p_first_name => v_first_name,
    p_last_name => v_last_name,
    p_hire_date => v_hire_date,
    p_salary => v_salary);
END;

This code will insert a new employee record into the "employees" table with the values specified in the variables.
