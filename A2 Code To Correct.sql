/*******************
Database Assignment 2

Group Members:
  - Minh Tam Nguyen
  - Mikael Ly
  - Xiaomei He
  - Elliot Josh

This script is a revision of the given Assignment 2 script. 
Here, we fixed any issues + added our recommended changes. 
Please see the Assignment 2 Document attached to see the logic + syntax error fixes.

The following is documentation explaining what the main purpose of this code is:
This script does the following:
  - Load unprocessed data and process them accordingly to given content:

  - Type of data that can be loaded:
    - Customer
    - Vendors
    - Category
    - Stock

  - Each data type can have the following functions done:
    - New (add new)
    - Status (change status)
    - Change (change information)

  - For any other data types, an error will be returned and processed.
  - For any other data functions not in the list, an error will be returned and processed.

  - The script loops over each given unprocessed data entry until all have been iterated through. 


*******************/

   set SERVEROUTPUT on;

declare
   k_customer         constant gggs_data_upload.data_type%type := 'CU';
   k_vendor           constant gggs_data_upload.data_type%type := 'VE';
   k_category         constant gggs_data_upload.data_type%type := 'CA';
   k_stock            constant gggs_data_upload.data_type%type := 'ST';
   k_new              constant gggs_data_upload.process_type%type := 'N';
   k_status           constant gggs_data_upload.process_type%type := 'S';
   k_change           constant gggs_data_upload.process_type%type := 'C';
   k_active_status    constant gggs_customer.status%type := 'A';
   k_data_processed   constant gggs_data_upload.data_processed%type := 'Y';
   k_data_unprocessed constant gggs_data_upload.data_processed%type := 'N';
   k_no_change_char   constant char(2) := 'NC';
   k_no_change_numb   constant number := -1;
   v_category_id      gggs_category.categoryid%type;
   v_vendor_id        gggs_vendor.vendorid%type;
   v_message          gggs_error_log_table.error_message%type;
   v_newvendorname    gggs_vendor.name%type;
   cursor c_gggs is
   select *
     from gggs_data_upload
    order by loadid;

begin

  -- For each item in the data table.
  -- r = Record
  -- c = ??? (Cursor?)
   for r_gggs in c_gggs loop
      savepoint before_row;
      begin
         dbms_output.put_line('Processing loadID='
                              || r_gggs.loadid
                              || ' data_type='
                              || r_gggs.data_type
                              || ' process_type=' || r_gggs.process_type);

      -- (Type of Customer Change?) If the data type is a Customer (CU),
      -- process the customer change methods
         if ( r_gggs.data_type = k_customer ) then

        -- (Add New Customer) If the processing type is New(N), add the new customer 
            if ( r_gggs.process_type = k_new ) then
               begin
                  insert into gggs_customer values ( gggs_customer_seq.nextval,
                                                     r_gggs.column1,
                                                     r_gggs.column2,
                                                     r_gggs.column3,
                                                     r_gggs.column4,
                                                     r_gggs.column5,
                                                     r_gggs.column6,
                                                     k_active_status );

               exception
                  when dup_val_on_index then
                     raise_application_error(
                        -20001,
                        'Duplicate Name found when adding new customer: ' || r_gggs.column1
                     );
               end;

        -- (Change Customer Status) If the processing type is Status(S), Change the customer status
            elsif ( r_gggs.process_type = k_status ) then
               update gggs_customer
                  set
                  status = r_gggs.column2
                where name = r_gggs.column1;
        
        -- (Change Requested Customer Information) If the processing type is Change(C), change information of a customer.
            elsif ( r_gggs.process_type = k_change ) then
               update gggs_customer
                  set province = decode(
                  r_gggs.column2,
                  k_no_change_char,
                  province,
                  r_gggs.column2
               ),
                      first_name = decode(
                         r_gggs.column3,
                         k_no_change_char,
                         first_name,
                         r_gggs.column3
                      ),
                      last_name = decode(
                         r_gggs.column4,
                         k_no_change_char,
                         last_name,
                         r_gggs.column4
                      ),
                      city = decode(
                         r_gggs.column5,
                         k_no_change_char,
                         city,
                         r_gggs.column5
                      ),
                      phone_number = nvl(
                         r_gggs.column6,
                         phone_number
                      ) -- should be NVL(r_gggs.column6, phone_number)
                where name = r_gggs.column1;  

        -- Otherwise, if any other processing type, process an error.
            else
               raise_application_error(
                  -20001,
                  r_gggs.process_type
                  || ' is not a valid process request for '
                  || r_gggs.data_type
                  || ' data'
               );
            end if;

      -- (Type of Vendor Change?) If the data type is a Vendor (VE)
         elsif ( r_gggs.data_type = k_vendor ) then

        -- (Add New Vendor) If the processing type is New(N)
            if ( r_gggs.process_type = k_new ) then
               begin
                  insert into gggs_vendor (
                     vendorid,
                     name,
                     description,
                     contact_first_name,
                     contact_last_name,
                     contact_phone_number,
                     status
                  ) values ( gggs_vendor_seq.nextval,
                             r_gggs.column1,
                             r_gggs.column2,
                             r_gggs.column3,
                             r_gggs.column4,
                             r_gggs.column6,
                             k_active_status );
               exception
                  when dup_val_on_index then
                     raise_application_error(
                        -20001,
                        'Duplicate Name found when adding new vendor: ' || r_gggs.column1
                     );
               end;
      

        -- (Change Vendor Status) If the processing type is Status(S)
            elsif ( r_gggs.process_type = k_status ) then
               update gggs_vendor
                  set
                  status = r_gggs.column2
                where name = r_gggs.column1;    
      
        -- (Change Requested Vendor Information) If the processing type is Change(C)
            elsif ( r_gggs.process_type = k_change ) then
               update gggs_vendor
                  set description = decode(
                  r_gggs.column2,
                  k_no_change_char,
                  description,
                  r_gggs.column2
               ),
                      contact_first_name = decode(
                         r_gggs.column3,
                         k_no_change_char,
                         contact_first_name,
                         r_gggs.column3
                      ),
                      contact_last_name = decode(
                         r_gggs.column4,
                         k_no_change_char,
                         contact_last_name,
                         r_gggs.column4
                      ),
                      contact_phone_number = nvl2(
                         r_gggs.column6,
                         r_gggs.column6,
                         contact_phone_number
                      )
                where name = r_gggs.column1;

        -- If any other processing type, process an error.
            else
               raise_application_error(
                  -20001,
                  r_gggs.process_type
                  || ' is not a valid process request for '
                  || r_gggs.data_type
                  || ' data'
               );
            end if;

      -- (Type of Category Change?) If the data type is Category (CA)
         elsif ( r_gggs.data_type = k_category ) then

        -- (Add New Category) If the processing type is New(N)
            if ( r_gggs.process_type = k_new ) then
               begin
                  insert into gggs_category values ( gggs_category_seq.nextval,
                                                     r_gggs.column1,
                                                     r_gggs.column2,
                                                     k_active_status );
               exception
                  when dup_val_on_index then
                     raise_application_error(
                        -20001,
                        'Duplicate Name found when adding new Category: ' || r_gggs.column1
                     );
               end;

        -- (Change Category Status) If the processing type is Status(S)    
            elsif ( r_gggs.process_type = k_status ) then
               update gggs_category
                  set
                  status = r_gggs.column2
                where name = r_gggs.column1;
        
        -- (Other / Process Error Displaying to Screen)
        -- If any other processing type, process an error.
            else
               raise_application_error(
                  -20001,
                  r_gggs.process_type
                  || ' is not a valid process request for '
                  || r_gggs.data_type
                  || ' data'
               );
            end if;

      -- (Type of Stock Change?) If the data type is Stock(ST)
         elsif ( r_gggs.data_type = k_stock ) then

        -- (Add New Stock) If the processing type is New(N)
            if ( r_gggs.process_type = k_new ) then
               select categoryid
                 into v_category_id
                 from gggs_category
                where name = r_gggs.column1;

               select vendorid
                 into v_vendor_id
                 from gggs_vendor
                where name = r_gggs.column2;

               begin
                  insert into gggs_stock values ( gggs_stock_seq.nextval,
                                                  v_category_id,
                                                  v_vendor_id,
                                                  r_gggs.column3,
                                                  r_gggs.column4,
                                                  r_gggs.column7,
                                                  r_gggs.column8,
                                                  k_active_status );

               exception
                  when dup_val_on_index then
                     raise_application_error(
                        -20001,
                        'Duplicate Name found when adding new Stock: ' || r_gggs.column3
                     );
               end;

        -- (Check Stock Status) If the processing type is Status(S)        
            elsif ( r_gggs.process_type = k_status ) then
               update gggs_stock
                  set
                  status = r_gggs.column2
                where name = r_gggs.column1;

        -- (Change Requested Stock Information) If the processing type is Change(C)
            elsif ( r_gggs.process_type = k_change ) then
               update gggs_stock
                  set description = decode(
                  r_gggs.column4,
                  k_no_change_char,
                  description,
                  r_gggs.column4
               ),
                      price = nvl2(
                         r_gggs.column7,
                         r_gggs.column7,
                         price
                      ),
                      no_in_stock = nvl2(
                         r_gggs.column8,
                         (no_in_stock - r_gggs.column8),
                         no_in_stock
                      )
                where name = r_gggs.column1;  
        
        -- (Other / Process Error Displaying to Screen)
        -- If any other processing type, process an error.
            else
               raise_application_error(
                  -20001,
                  r_gggs.process_type
                  || ' is not a valid process request for '
                  || r_gggs.data_type
                  || ' data'
               );
            end if;
    
    -- (Other / Process Error Displaying to Screen)
    -- If the data type is anything other than these, process an error.
         else
            raise_application_error(
               -20000,
               r_gggs.data_type || ' is not a valid type of data to process'
            );
         end if;

         update gggs_data_upload
            set
            data_processed = k_data_processed
          where loadid = r_gggs.loadid;


      exception
         when others then
            rollback to before_row;
            v_message := sqlerrm;
            insert into gggs_error_log_table values ( r_gggs.data_type,
                                                      r_gggs.process_type,
                                                      v_message );

            dbms_output.put_line(v_message);
      end;
   end loop;

   commit;
end;
/