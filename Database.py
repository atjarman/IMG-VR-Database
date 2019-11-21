import psycopg2
from Bio import SeqIO

#   Creates new metadata and sequence tables
def create_tables():
    """ create tables in the PostgreSQL database"""
    commands = (
        """
        CREATE TABLE metadata (
            UViG VARCHAR(255) PRIMARY KEY,
            TAXON_OID VARCHAR(255) NOT NULL,
            Scaffold_ID VARCHAR(255) NOT NULL
        )
        """,
        """
        CREATE TABLE sequence (
            UViG VARCHAR(255) PRIMARY KEY,
            Sequence TEXT NOT NULL,
            FOREIGN KEY (UViG)
                    REFERENCES sequence (UViG)
        )
        """)
    conn = None
    try:
        # read the connection parameters
        #params = config()
        params = "dbname='jgi' user='postgres' host='localhost' password='Peru2013!'"
        # connect to the PostgreSQL server
        #conn = psycopg2.connect(**params)
        conn = psycopg2.connect(params)
        cur = conn.cursor()
        # create table one by one
        for command in commands:
            cur.execute(command)
        #cur.execute(commands)
        # close communication with the PostgreSQL database server
        cur.close()
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

#   First upload of metadata using the orignal_Metadata_file method data (Only used when first uploading the original data)
def metadata_Upload(UViG,TAXON_OID,Scaffold_ID):
    """ insert a new vendor into the vendors table """
    sql = """INSERT INTO metadata(UViG,TAXON_OID,Scaffold_ID)
             VALUES(%s,%s,%s) RETURNING UViG,TAXON_OID,Scaffold_ID;"""
    conn = None
    metadata_id = None
    try:
        # read database configuration
        #params = config()
        params = "dbname='jgi' user='postgres' host='localhost' password='Peru2013!'"
        # connect to the PostgreSQL database
        #conn = psycopg2.connect(**params)
        conn = psycopg2.connect(params)
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        cur.execute(sql, (UViG,TAXON_OID,Scaffold_ID))
        # get the generated id back
        metadata_id = cur.fetchone()[0]
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

    return metadata_id

#   Command that takes the file path of the metadata to be imported (Only used when first importing the original data)
def original_Metadata_file(file):
    counter1 = 0
    masterList = []
    headerList1 = []

    for line in file:
        if counter1 == 0:
            line = line.strip('\n')
            headerList1 = line.split('\t')
            counter1 += 1
        else:
            UViG = "UViG"
            TAXON_OID = "TAXON_OID"
            Scaffold_ID = "Scaffold_ID"
            line = line.strip('\n')
            lineList1 = line.split('\t')

            UViG = lineList1[(headerList1.index(UViG))]
            TAXON_OID = lineList1[(headerList1.index(TAXON_OID))]
            Scaffold_ID = lineList1[(headerList1.index(Scaffold_ID))]
            metadata_Upload(UViG,TAXON_OID,Scaffold_ID)

#   First upload of sequence data using the sequences method data (Only used when first uploading the sequence data)
def sequence_Upload(UViG,Sequence):
    """ insert a new vendor into the vendors table """
    sql = """INSERT INTO sequence(UViG,Sequence)
             VALUES(%s,%s) RETURNING UViG,Sequence;"""
    conn = None
    sequence_id = None
    try:
        # read database configuration
        #params = config()
        params = "dbname='jgi' user='postgres' host='localhost' password='Peru2013!'"
        # connect to the PostgreSQL database
        #conn = psycopg2.connect(**params)
        conn = psycopg2.connect(params)
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        cur.execute(sql, (UViG,Sequence))
        # get the generated id back
        sequence_id = cur.fetchone()[0]
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

    return sequence_id

#   Command that takes the file path of the sequence data to be imported (Only used when first importing the sequence data)
def sequences(file):
    fasta_sequences = SeqIO.parse(open(file),'fasta')
    count = 0
    for fasta in fasta_sequences:
        if count <= 50:
            name, sequence = fasta.id, str(fasta.seq)
            print(name)
            print(sequence)
            sequence_Upload(name,sequence)
        else:
            break
        count += 1

#   Updates metadata (Used for each data upload after the first original upload)
def update_Metadata(UViG,TAXON_OID,Scaffold_ID):
    """ update vendor name based on the vendor id """
    sql = """ UPDATE metadata
                SET TAXON_OID = %s,
                Scaffold_ID = %s
                WHERE UViG = %s"""
    conn = None
    updated_rows = 0
    try:
        # read database configuration
        params = "dbname='jgi' user='postgres' host='localhost' password='Peru2013!'"
        # connect to the PostgreSQL server
        #conn = psycopg2.connect(**params)
        conn = psycopg2.connect(params)
        # create a new cursor
        cur = conn.cursor()
        # execute the UPDATE  statement
        cur.execute(sql, (TAXON_OID,Scaffold_ID,UViG))
        # get the number of updated rows
        updated_rows = cur.rowcount
        # Commit the changes to the database
        conn.commit()
        # Close communication with the PostgreSQL database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

    return updated_rows

#
def new_Metadata_File(file):
    counter2 = 0
    for line in file:
        if counter2 == 0:
            line = line.strip('\n')
            headerList1 = line.split('\t')
            counter2 += 1
        else:
            UViG = "UViG"
            TAXON_OID = "TAXON_OID"
            Scaffold_ID = "Scaffold_ID"

            line = line.strip('\n')
            lineList1 = line.split('\t')

            UViG = lineList1[(headerList1.index(UViG))]
            TAXON_OID = lineList1[(headerList1.index(TAXON_OID))]
            Scaffold_ID = lineList1[(headerList1.index(Scaffold_ID))]

            update_Metadata(UViG,TAXON_OID,Scaffold_ID)

#   Metadata file location
#   Example fileMD = open('C:/.../IMGVR_all_Sequence_information.tsv', 'r')

#fileMD = open('INSERT FILEPATH HERE','r')

#   Fasta file location
#   Example fileFasta = "C:/.../IMG_VR_Jan18.fna"

#fileMD = "INSERT FILEPATH HERE"

fileMD = open('C:/Users/Caitlin/Desktop/BMI 483 (Capstone II)/Test Files/IMGVR_all_Sequence_information.tsv.txt', 'r')
fileFasta = "C:\\Users\\Caitlin\\Desktop\\BMI 483 (Capstone II)\\IMG_VR_download\\IMG_VR\\IMG_VR_2018-01-01_3\\IMG_VR_Jan18.fna"

new_Metadata_File(fileMD)