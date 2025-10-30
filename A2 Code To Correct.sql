set SERVEROUTPUT on;

DECLARE
      
  k_customer          CONSTANT    gggs_data_upload.data_type%TYPE := 'CU';
  k_vendor            CONSTANT    gggs_data_upload.data_type%TYPE := 'VE';
  k_category          CONSTANT    gggs_data_upload.data_type%TYPE := 'CA';
  k_stock             CONSTANT    gggs_data_upload.data_type%TYPE := 'ST';  
  k_new               CONSTANT    gggs_data_upload.process_type%TYPE := 'N';
  k_status            CONSTANT    gggs_data_upload.process_type%TYPE := 'S';
  k_change            CONSTANT    gggs_data_upload.process_type%TYPE := 'C'; 
  k_active_status     CONSTANT    gggs_customer.status%TYPE := 'A';
  k_data_processed    CONSTANT    gggs_data_upload.data_processed%TYPE := 'Y';
  k_data_unprocessed  CONSTANT    gggs_data_upload.data_processed%TYPE := 'N';
  k_no_change_char    CONSTANT    CHAR(2) := 'NC';
  k_no_change_numb    CONSTANT    NUMBER := -1;  
  v_category_id                   gggs_category.categoryID%TYPE;
  v_vendor_id                     gggs_vendor.vendorID%TYPE;
  v_message                       gggs_error_log_table.error_message%TYPE;  
  v_newVendorName                 gggs_vendor.NAME%TYPE;

  CURSOR c_gggs IS
    SELECT *
      FROM gggs_data_upload
	 ORDER BY loadID;  

BEGIN

  -- For each item in the data table.
  -- r = Record
  -- c = ??? (Cursor?)
  FOR r_gggs IN c_gggs LOOP
    SAVEPOINT before_row;
    BEGIN 
      DBMS_OUTPUT.PUT_LINE('Processing loadID=' || r_gggs.loadID ||
                     ' data_type=' || r_gggs.data_type ||
                     ' process_type=' || r_gggs.process_type);

      -- (Type of Customer Change?) If the data type is a Customer (CU),
      -- process the customer change methods
      IF (r_gggs.data_type = k_customer) THEN

        -- (Add New Customer) If the processing type is New(N), add the new customer 
        IF (r_gggs.process_type = k_new) THEN
          INSERT INTO gggs_customer
          VALUES (gggs_customer_seq.NEXTVAL, r_gggs.column1, r_gggs.column2, r_gggs.column3,
                  r_gggs.column4, r_gggs.column5, r_gggs.column6, k_active_status);

        -- (Change Customer Status) If the processing type is Status(S), Change the customer status
        ELSIF (r_gggs.process_type = k_status) THEN
          UPDATE gggs_customer
             SET status = r_gggs.column2
           WHERE name = r_gggs.column1;
        
        -- (Change Requested Customer Information) If the processing type is Change(C), change information of a customer.
        ELSIF (r_gggs.process_type = k_change) THEN
          UPDATE gggs_customer
             SET province = DECODE(r_gggs.column2, k_no_change_char, province, r_gggs.column2),
                 first_name = DECODE(r_gggs.column3, k_no_change_char, first_name, r_gggs.column3),
                 last_name = DECODE(r_gggs.column4, k_no_change_char, last_name, r_gggs.column4),
                 city = DECODE(r_gggs.column5, k_no_change_char, city, r_gggs.column5),
                 phone_number = NVL(r_gggs.column6, phone_number) -- should be NVL(r_gggs.column6, phone_number)
           WHERE name = r_gggs.column1;  

        -- Otherwise, if any other processing type, process an error.
   	    ELSE 
	      RAISE_APPLICATION_ERROR(-20001, r_gggs.process_type || ' is not a valid process request for ' || r_gggs.data_type || ' data');
        END IF;

      -- (Type of Vendor Change?) If the data type is a Vendor (VE)
      ELSIF (r_gggs.data_type = k_vendor) THEN

        -- (Add New Vendor) If the processing type is New(N)
  IF (r_gggs.process_type = k_new) THEN
      INSERT INTO gggs_vendor(vendorID, name, description, contact_first_name,
                              contact_last_name, contact_phone_number, status)
      VALUES (gggs_vendor_seq.NEXTVAL,
              r_gggs.column1,
              r_gggs.column2,
              r_gggs.column3,
              r_gggs.column4,
              r_gggs.column6,
              k_active_status); 

        -- (Change Vendor Status) If the processing type is Status(S)
        ELSIF (r_gggs.process_type = k_status) THEN
          UPDATE gggs_vendor
             SET status = r_gggs.column2
           WHERE name = r_gggs.column1;    
      
        -- (Change Requested Vendor Information) If the processing type is Change(C)
        ELSIF (r_gggs.process_type = k_change) THEN
          UPDATE gggs_vendor
             SET description = DECODE(r_gggs.column2, k_no_change_char, description, r_gggs.column2),
                 contact_first_name = DECODE(r_gggs.column3, k_no_change_char, contact_first_name, r_gggs.column3),
                 contact_last_name = DECODE(r_gggs.column4, k_no_change_char, contact_last_name, r_gggs.column4),
                 contact_phone_number = NVL2(r_gggs.column6, r_gggs.column6, contact_phone_number)
           WHERE name = r_gggs.column1 ;

        -- If any other processing type, process an error.
        ELSE 
	      RAISE_APPLICATION_ERROR(-20001, r_gggs.process_type || ' is not a valid process request for ' || r_gggs.data_type || ' data');
        END IF;

      -- (Type of Category Change?) If the data type is Category (CA)
      ELSIF (r_gggs.data_type = k_category) THEN

        -- (Add New Category) If the processing type is New(N)
        IF (r_gggs.process_type = k_new) THEN
          INSERT INTO gggs_category
          VALUES (gggs_category_seq.NEXTVAL, r_gggs.column1, r_gggs.column2, k_active_status);

        -- (Change Category Status) If the processing type is Status(S)    
        ELSIF (r_gggs.process_type = k_status) THEN
          UPDATE gggs_category
             SET status = r_gggs.column2
           WHERE name = r_gggs.column1;
        
        -- (Other / Process Error Displaying to Screen)
        -- If any other processing type, process an error.
        ELSE 
	      RAISE_APPLICATION_ERROR(-20001, r_gggs.process_type || ' is not a valid process request for ' || r_gggs.data_type || ' data');
        END IF;

      -- (Type of Stock Change?) If the data type is Stock(ST)
      ELSIF (r_gggs.data_type = k_stock) THEN

        -- (Add New Stock) If the processing type is New(N)
        IF (r_gggs.process_type = k_new) THEN
          SELECT categoryID
            INTO v_category_id
            FROM gggs_category
           WHERE name = r_gggs.column1; 
         
          SELECT vendorID
            INTO v_vendor_id
            FROM gggs_vendor
           WHERE name = r_gggs.column2;
      
          INSERT INTO gggs_stock
          VALUES (gggs_stock_seq.NEXTVAL, v_category_id, v_vendor_id, r_gggs.column3,
                  r_gggs.column4, r_gggs.column7, r_gggs.column8, k_active_status);

        -- (Check Stock Status) If the processing type is Status(S)        
        ELSIF (r_gggs.process_type = k_status) THEN
          UPDATE gggs_stock
             SET status = r_gggs.column2
           WHERE name = r_gggs.column1;

        -- (Change Requested Stock Information) If the processing type is Change(C)
        ELSIF (r_gggs.process_type = k_change) THEN
          UPDATE gggs_stock
             SET description = DECODE(r_gggs.column4, k_no_change_char, description, r_gggs.column4),
                 price = NVL2(r_gggs.column7, r_gggs.column7, price),
                 no_in_stock = NVL2(r_gggs.column8, (no_in_stock - r_gggs.column8), no_in_stock)
           WHERE name = r_gggs.column1;  
        
        -- (Other / Process Error Displaying to Screen)
        -- If any other processing type, process an error.
        ELSE 
	        RAISE_APPLICATION_ERROR(-20001, r_gggs.process_type || ' is not a valid process request for ' || r_gggs.data_type || ' data');
        END IF;
    
    -- (Other / Process Error Displaying to Screen)
    -- If the data type is anything other than these, process an error.
	    ELSE 
	      RAISE_APPLICATION_ERROR(-20000, r_gggs.data_type || ' is not a valid type of data to process');
      END IF;
    
      UPDATE gggs_data_upload
	     SET data_processed = k_data_processed
	   WHERE loadID = r_gggs.loadID;
	 
	
    EXCEPTION 
      WHEN OTHERS THEN 
        ROLLBACK TO before_row; 

        v_message := SQLERRM;

        INSERT INTO  gggs_error_log_table
        VALUES
         (r_gggs.data_type, r_gggs.process_type, v_message);
	   
        DBMS_OUTPUT.PUT_LINE(v_message);
      END;
  END LOOP;  
	  
	      COMMIT;
END;
/