-- Targets ConstructTag.UTL_FILE — canonical example flags this as
-- needs-review and recommends moving the export out of the database
-- entirely (psql \copy, COPY TO STDOUT, or a script).

CREATE OR REPLACE PROCEDURE export_employees_to_csv(
    p_dir       IN VARCHAR2,
    p_filename  IN VARCHAR2
) IS
    f UTL_FILE.FILE_TYPE;
BEGIN
    f := UTL_FILE.FOPEN(p_dir, p_filename, 'W');
    FOR rec IN (SELECT employee_id, last_name, hire_date FROM employees) LOOP
        UTL_FILE.PUT_LINE(
            f,
            rec.employee_id || ',' || rec.last_name || ',' || rec.hire_date
        );
    END LOOP;
    UTL_FILE.FCLOSE(f);
EXCEPTION
    WHEN OTHERS THEN
        IF UTL_FILE.IS_OPEN(f) THEN UTL_FILE.FCLOSE(f); END IF;
        RAISE;
END export_employees_to_csv;
/
