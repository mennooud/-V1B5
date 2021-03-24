import psycopg2

c = psycopg2.connect("dbname=testtest user=postgres password=broodje123")
cur = c.cursor()

filenames = ['products']

for filename in filenames:
    with open(filename+'.csv') as csvfile:
        print("Copying {}...".format(filename))
        cur.copy_expert("COPY "+filename+" FROM STDIN DELIMITER ',' CSV HEADER", csvfile)
        c.commit()

c.commit()
cur.close()
c.close()