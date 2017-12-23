import psycopg2 
import re
from config import config

out_file = open('gnafaddresses.txt', 'w')

def get_addresses():
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute("SELECT a.address_detail_pid,\
							a.building_name,\
							a.flat_type_code,\
							a.flat_number_prefix,\
							a.flat_number,\
							a.flat_number_suffix,\
							a.level_type_code,\
							a.level_number_prefix,\
							a.level_number,\
							a.level_number_suffix,\
							a.number_first_prefix,\
							a.number_first,\
							a.number_first_suffix,\
							a.number_last_prefix,\
							a.number_last,\
							a.number_last_suffix,\
							b.street_name,\
							b.street_type_code,\
							b.street_suffix_code,\
							c.locality_name,\
							a.postcode,\
							d.longitude,\
							d.latitude,\
							f.mb_2016_code\
							from raw_gnaf_201711.address_detail a,\
							     raw_gnaf_201711.street_locality b,\
							     raw_gnaf_201711.locality c,\
							     raw_gnaf_201711.address_site_geocode d,\
							     raw_gnaf_201711.address_mesh_block_2016 e,\
							     raw_gnaf_201711.mb_2016 f\
							where a.street_locality_pid = b.street_locality_pid and\
							      c.locality_pid = b.locality_pid and\
							      a.address_site_pid = d.address_site_pid and\
							      a.address_detail_pid = e.address_detail_pid and\
							      e.mb_2016_pid = f.mb_2016_pid")

        print("number of addresses: ", cur.rowcount)
        row = cur.fetchone()
        ctr = 1
        while row is not None:      

        	address_detail_pid = row[0]
        	if address_detail_pid is None: 
        		address_detail_pid = ''

       		building_name = row[1]
        	if building_name is None:
        		building_name = ''

       		flat_type_code = row[2]
        	if flat_type_code is None:
        		flat_type_code = ''

       		flat_number_prefix = row[3]
        	if flat_number_prefix is None:
        		flat_number_prefix = ''

       		flat_number_dec = row[4]
        	if flat_number_dec is None:
        		flat_number_dec = ''	        		
        	flat_number = str(flat_number_dec)	

       		flat_number_suffix = row[5]
        	if flat_number_suffix is None:
        		flat_number_suffix = ''

        	level_type_code = row[6]
        	if level_type_code is None:
	        	level_type_code = ''

       		level_number_prefix = row[7]
	        if level_number_prefix is None:
	        	level_number_prefix = ''

       		level_number_dec = row[8]
        	if level_number_dec is None:
        		level_number_dec = ''        		
       		level_number = str(level_number_dec)

       		level_number_suffix = row[9]
        	if level_number_suffix is None:
        		level_number_suffix = ''

       		number_first_prefix = row[10]
        	if number_first_prefix is None:
        		number_first_prefix = ''

       		number_first_dec = row[11]
        	if number_first_dec is None:        		
        		number_first_dec = ''	
       		number_first =str(number_first_dec)

       		number_first_suffix = row[12]
        	if number_first_suffix is None:
        		number_first_suffix = ''

       		number_last_prefix = row[13]
        	if number_last_prefix is None:
        		number_last_prefix = ''

       		number_last_dec = row[14]
        	if number_last_dec is None:
        		number_last_dec = ''
       		number_last = str(number_last_dec)

       		number_last_suffix = row[15]
        	if number_last_suffix is None:
        		number_last_suffix = ''

       		street_name = row[16]
        	if street_name is None:        		
        		street_name = ''

       		street_type_code = row[17]
        	if street_type_code is None:
        		street_type_code = ''

       		street_suffix_code = row[18]
        	if street_suffix_code is None:
        		street_suffix_code = ''

       		locality_name = row[19]
        	if locality_name is None:
        		locality_name = ''

       		postcode_dec = row[20]
        	if postcode_dec is None:
        		postcode_dec = ''
       		postcode = str(postcode_dec)

       		longitude_dec = row[21]
        	if longitude_dec is None:
        		longitude_dec = ''
       		longitude = str(longitude_dec)

       		latitude_dec = row[22] 
        	if latitude_dec is None:
        		latitude_dec = ''
       		latitude = str(latitude_dec)

       		mb_2016_code_dec = row[23]
        	if mb_2016_code_dec is None:
        		mb_2016_code_dec = ''
       		mb_2016_code = str(mb_2016_code_dec)
       		
       		cleansed_address_temp = (building_name +" "+\
		        	flat_type_code +" "+\
		        	flat_number_prefix +\
		        	flat_number +\
		        	flat_number_suffix +" "+\
		        	level_type_code +" "+\
		        	level_number_prefix +\
		        	level_number +\
		        	level_number_suffix +" "+\
		        	number_first_prefix +\
		        	number_first +\
		        	number_first_suffix +" "+\
		        	number_last_prefix +\
		        	number_last +\
		        	number_last_suffix +" "+\
		        	street_name +" "+\
		        	street_type_code +" "+\
		        	street_suffix_code +" "+\
		        	locality_name +" "+\
		        	"NSW" +" "+\
		        	postcode)
        	cleansed_address = re.sub(' +',' ',cleansed_address_temp).strip()

        	out_file.write("db.session.add(nsw_addresses(" +\
        				  "\""+address_detail_pid +"\","+\
        				  "\""+cleansed_address +"\","+\
						  "\""+locality_name +"\","+\
						  "\""+postcode +"\","+\
						  "\""+longitude +"\","+\
						  "\""+latitude +"\","+\
						  "\""+mb_2016_code +"\")\n")
        	ctr+=1
        	print("Number of records written :", ctr)
        	row = cur.fetchone()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

if __name__ == '__main__':    
    get_addresses()