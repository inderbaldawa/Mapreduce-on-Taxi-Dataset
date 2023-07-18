
import happybase
#create connection
connection = happybase.Connection('mydb.czmggnaqbkcy.us-east-1.rds.amazonaws.com', port=3306,
autoconnect=False)
#open connection to perform operations
def open_connection():
	connection.open()
#close the opened connection
def close_connection():
	connection.close()
#get the pointer to a table
def get_table(name):
	open_connection()
	table = connection.table(name)
	close_connection()
	return table
#batch insert data in events table
def batch_insert_data(filename):
	print("starting batch insert of events")
	file = open(filename, "r")
	table = get_table(filename)
	open_connection()
	i = 0
	with table.batch(batch_size=2) as b:
		for line in file:
			if i!=0:
				temp = line.strip().split(",")
				# this put() will result in two mutations (two cells)
				b.put(temp[1]+":"+temp[0] , { 'type:'+temp[3]: '1' })
			i+=1
	file.close()
	print("batch insert done")
	close_connection()

batch_insert_data('yellow_tripdata_2017-03.csv')
batch_insert_data('yellow_tripdata_2017-04.csv')