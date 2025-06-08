#!/usr/bin/python3
#
# Interface for the assignment
#

import psycopg2
import time

def getopenconnection(user='postgres', password='chien1472004', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

def create_metadata_table(openconnection):
    cur = openconnection.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS partition_metadata (
            partition_type VARCHAR(20),
            partition_count INTEGER,
            current_partition INTEGER,
            range_size FLOAT
        )
    """)
    openconnection.commit()
    cur.close()

def loadratings3(ratingstablename, ratingsfilepath, openconnection):
    start_time = time.time() 
    cur = openconnection.cursor()
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {ratingstablename} (
            userid INTEGER,
            movieid INTEGER,
            rating FLOAT
        )
    """)

    import tempfile

    with tempfile.NamedTemporaryFile(mode='w+', delete=True) as tmpfile:
        with open(ratingsfilepath, 'r') as f:
            for line in f:
                parts = line.strip().split('::')
                if len(parts) >= 3:
                    userid, movieid, rating = parts[0], parts[1], parts[2]
                    tmpfile.write(f"{userid}:{movieid}:{rating}\n")
        tmpfile.flush()
        tmpfile.seek(0)

        cur.copy_from(tmpfile, ratingstablename, sep=':', columns=('userid', 'movieid', 'rating'))

    openconnection.commit()
    cur.close()

    end_time = time.time()
    print(f"Loaded data into '{ratingstablename}' in {end_time - start_time:.2f} seconds.")

def loadratings(ratingstablename, ratingsfilepath, openconnection): 
    con = openconnection
    cur = con.cursor()
    import time
    start_time = time.time()

    # Tạo bảng với các cột thừa 
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {ratingstablename} (
            userid INTEGER,
            extra1 CHAR,
            movieid INTEGER,
            extra2 CHAR,
            rating FLOAT,
            extra3 CHAR,
            timestamp BIGINT
        );
    """)

    # Chèn dữ liệu vào trong bảng 
    with open(ratingsfilepath, 'r') as infile:
        cur.copy_from(infile, ratingstablename, sep=':')

    # Xóa bỏ các cột dư thừa
    cur.execute(f"""
        ALTER TABLE {ratingstablename}
        DROP COLUMN extra1,
        DROP COLUMN extra2,
        DROP COLUMN extra3,
        DROP COLUMN timestamp;
    """)
    elapsed_time = time.time() - start_time
    print(f"loadratings executed in {elapsed_time:.2f} seconds")

    con.commit()
    cur.close()

def loadratings2(ratingstablename, ratingsfilepath, openconnection):
    cur = openconnection.cursor()
    # Tạo bảng nếu chưa có
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {ratingstablename} (
            userid INTEGER,
            movieid INTEGER,
            rating FLOAT
        )
    """)
    openconnection.commit()
    inserted_rows = 0

    with open(ratingsfilepath, 'r') as file:
        for line_num, line in enumerate(file, 1):
            parts = line.strip().split('::')
            if len(parts) >= 3:
                try:
                    userid = int(parts[0])
                    movieid = int(parts[1])
                    rating = float(parts[2])
                    cur.execute(f"""
                        INSERT INTO {ratingstablename} (userid, movieid, rating)
                        VALUES (%s, %s, %s)
                    """, (userid, movieid, rating))
                    inserted_rows += 1
                    if inserted_rows % 1000 == 0:
                        openconnection.commit()
                except Exception as e:
                    print(f"⚠️ Error at line {line_num}: {e}")

    openconnection.commit()
    cur.close()
    print(f"✅ Inserted {inserted_rows} rows into '{ratingstablename}'")

def rangepartition2(ratingstablename, numberofpartitions, openconnection):
    import time
    start_time = time.time()
    cur = openconnection.cursor()
    RANGE_TABLE_PREFIX = 'range_part'

    range_size = 5.0 / numberofpartitions
    
    create_metadata_table(openconnection)
    
    cur.execute("""
        INSERT INTO partition_metadata (partition_type, partition_count, range_size)
        VALUES ('range', %s, %s)
    """, (numberofpartitions, range_size))
    
    for i in range(numberofpartitions):
        min_rating = i * range_size
        max_rating = min_rating + range_size
        if i==numberofpartitions-1: max_rating=5
        
        table_name = f"{RANGE_TABLE_PREFIX}{i}"
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                userid INTEGER,
                movieid INTEGER,
                rating FLOAT
            )
        """)
        if i == 0:
            cur.execute(f"""
                INSERT INTO {table_name}
                SELECT userid, movieid, rating
                FROM {ratingstablename}
                WHERE rating >= {min_rating} AND rating <= {max_rating}
            """)
        else:
            cur.execute(f"""
                INSERT INTO {table_name}
                SELECT userid, movieid, rating
                FROM {ratingstablename}
                WHERE rating > {min_rating} AND rating <= {max_rating}
            """)
    elapsed_time = time.time() - start_time
    print(f"rangpatition executed in {elapsed_time:.2f} seconds")
    openconnection.commit()
    cur.close()

def rangepartition(ratingstablename, numberofpartitions, openconnection):
    import time
    start_time = time.time()
    cur = openconnection.cursor()
    RANGE_TABLE_PREFIX = 'range_part'
    
    # Tính kích thước chênh lệch cao nhất giữa các phân vùng
    range_size = 5.0 / numberofpartitions
    
    # Tạo bảng meta data 
    create_metadata_table(openconnection)

    # Chèn thông tin về phân vùng kiểu range vào bên trong bảng meta data
    cur.execute("""
        INSERT INTO partition_metadata (partition_type, partition_count, range_size)
        VALUES ('range', %s, %s)
    """, (numberofpartitions, range_size))

    # Lặp để tạo và chèn các bản ghi vào trong bảng
    for i in range(numberofpartitions):
        min_rating = i * range_size

        # Kiểm tra xem có phải là bảng cuối cùng hay không, bảng cuối giá trị cao nhất phải là 5
        max_rating = 5.0 if i == numberofpartitions - 1 else min_rating + range_size
        
        table_name = f"{RANGE_TABLE_PREFIX}{i}"
        
        condition = f"rating >= {min_rating} AND rating <= {max_rating}" if i == 0 \
            else f"rating > {min_rating} AND rating <= {max_rating}"
        
        #Thực hiện lệnh SQL
        cur.execute(f"""
            CREATE TABLE {table_name} AS
            SELECT userid, movieid, rating
            FROM {ratingstablename}
            WHERE {condition}
        """)
    
    openconnection.commit()
    cur.close()
    elapsed_time = time.time() - start_time
    print(f"rangepartition executed in {elapsed_time:.2f} seconds")

def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    import time
    start_time = time.time()
    cur = openconnection.cursor()
    RANGE_TABLE_PREFIX = 'range_part'
    
    # Lấy thông tin phân vùng kiểu range trong bảng meta data
    cur.execute("""
        SELECT partition_count, range_size 
        FROM partition_metadata 
        WHERE partition_type = 'range'
    """)
    partition_info = cur.fetchone()
    if not partition_info:
        raise Exception("No range partitioning information found")
    
    numberofpartitions, range_size = partition_info
    
    partition_num = int(rating / range_size)
    
    # Kiểm tra nếu chia hết thì phải lùi lại 1 bảng
    if rating % range_size == 0 and partition_num > 0:
        partition_num -= 1
    
    #Chèn vào bảng chính trước
    cur.execute(f"""
        INSERT INTO {ratingstablename} (userid, movieid, rating)
        VALUES (%s, %s, %s)
    """, (userid, itemid, rating))

    # Chèn vào phân mảnh 
    partition_table = f"{RANGE_TABLE_PREFIX}{partition_num}"
    cur.execute(f"""
        INSERT INTO {partition_table} (userid, movieid, rating)
        VALUES (%s, %s, %s)
    """, (userid, itemid, rating))

    elapsed_time = time.time() - start_time
    print(f"ranginsert executed in {elapsed_time:.2f} seconds")
    openconnection.commit()
    cur.close()

def roundrobinpartition1(ratingstablename, numberofpartitions, openconnection):
    import time
    cur = openconnection.cursor()
    RROBIN_TABLE_PREFIX = 'rrobin_part'

    start_time = time.time()  # Bắt đầu tính thời gian

    create_metadata_table(openconnection)
    
    cur.execute("""
        INSERT INTO partition_metadata (partition_type, partition_count, current_partition)
        VALUES ('roundrobin', %s, 0)
    """, (numberofpartitions,))
    
    for i in range(numberofpartitions):
        table_name = f"{RROBIN_TABLE_PREFIX}{i}"
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                userid INTEGER,
                movieid INTEGER,
                rating FLOAT
            )
        """)

    for i in range(numberofpartitions):
        table_name = f"{RROBIN_TABLE_PREFIX}{i}"
        cur.execute(f"""
            INSERT INTO {table_name}
            SELECT userid, movieid, rating
            FROM (
                SELECT userid, movieid, rating,
                       ROW_NUMBER() OVER (ORDER BY userid, movieid) - 1 as row_num
                FROM {ratingstablename}
            ) numbered_rows
            WHERE row_num % {numberofpartitions} = {i}
        """)

    query = f"""
        UPDATE partition_metadata
        SET current_partition = (
            SELECT COUNT(*) % {numberofpartitions} FROM {ratingstablename}
        )
        WHERE partition_type = 'roundrobin'
    """
    cur.execute(query)
    
    openconnection.commit()
    cur.close()

    end_time = time.time()  # Kết thúc thời gian
    elapsed_time = end_time - start_time
    print(f"roundrobinpartition executed in {elapsed_time:.2f} seconds")

def roundrobinpartition3(ratingstablename, numberofpartitions, openconnection):
    import time
    cur = openconnection.cursor()
    RROBIN_TABLE_PREFIX = 'rrobin_part'

    start_time = time.time()

    create_metadata_table(openconnection)

    # Ghi metadata
    cur.execute("""
        INSERT INTO partition_metadata (partition_type, partition_count, current_partition)
        VALUES ('roundrobin', %s, 0)
    """, (numberofpartitions,))

    # Tạo bảng đích
    for i in range(numberofpartitions):
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {RROBIN_TABLE_PREFIX}{i} (
                userid INTEGER,
                movieid INTEGER,
                rating FLOAT
            )
        """)

    # ✅ Tạo bảng tạm chứa row_number 1 lần
    cur.execute(f"""
        CREATE TEMP TABLE temp_numbered AS
        SELECT userid, movieid, rating,
               ROW_NUMBER() OVER () - 1 AS row_num
        FROM {ratingstablename}
    """)

    # ✅ Chia dữ liệu từ bảng tạm vào các bảng phân vùng
    for i in range(numberofpartitions):
        cur.execute(f"""
            INSERT INTO {RROBIN_TABLE_PREFIX}{i}
            SELECT userid, movieid, rating
            FROM temp_numbered
            WHERE row_num % {numberofpartitions} = {i}
        """)

    # Cập nhật metadata
    cur.execute(f"""
        UPDATE partition_metadata
        SET current_partition = (
            SELECT COUNT(*) % {numberofpartitions} FROM temp_numbered
        )
        WHERE partition_type = 'roundrobin'
    """)

    openconnection.commit()
    cur.close()

    elapsed_time = time.time() - start_time
    print(f"[Cách 2] roundrobinpartition executed in {elapsed_time:.2f} seconds")


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    import time
    start_time = time.time()
    cur = openconnection.cursor()
    RROBIN_TABLE_PREFIX = 'rrobin_part'
    
    # Chèn bản ghi vào bảng chính trước
    cur.execute(f"""
        INSERT INTO {ratingstablename} (userid, movieid, rating)
        VALUES (%s, %s, %s)
    """, (userid, itemid, rating))
    
    # Lấy thông tin phân vùng round robin đồng thời cập nhật trong cùng 1 truy vấn
    cur.execute("""
        UPDATE partition_metadata
        SET current_partition = (current_partition + 1) % partition_count
        WHERE partition_type = 'roundrobin'
        RETURNING ((current_partition - 1 + partition_count) % partition_count) AS old_partition,
                  partition_count
    """)
    old_partition, numberofpartitions = cur.fetchone()
    
    #Chèn vào bảng phân mảnh
    partition_table = f"{RROBIN_TABLE_PREFIX}{old_partition}"
    cur.execute(f"""
        INSERT INTO {partition_table} (userid, movieid, rating)
        VALUES (%s, %s, %s)
    """, (userid, itemid, rating))
    
    elapsed_time = time.time() - start_time
    print(f"rourobin insert executed in {elapsed_time:.2f} seconds")
    openconnection.commit()
    cur.close()


def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    import time
    start_time = time.time()
    cur = openconnection.cursor()
    RROBIN_TABLE_PREFIX = 'rrobin_part'

    create_metadata_table(openconnection)

    #Tạo bảng tạm thời để lưu thông tin kèm số thứ tự hàng của các bản ghi 
    cur.execute(f"""
        CREATE TEMP TABLE temp_numbered AS
        SELECT
            userid,
            movieid,
            rating,
            ROW_NUMBER() OVER () - 1 AS row_num
        FROM {ratingstablename};
    """)

    #Lặp tạo bảng đồng thời chèn luôn các bản ghi thỏa mãn
    for i in range(numberofpartitions):
        table_name = f"{RROBIN_TABLE_PREFIX}{i}"
        cur.execute(f"""
            CREATE TABLE {table_name} AS
            SELECT userid, movieid, rating
            FROM temp_numbered
            WHERE row_num % {numberofpartitions} = {i};
        """)

    # Chèn vào bảng meta data thông tin phân vùng của round robin
    cur.execute(f"""
                    INSERT INTO partition_metadata (partition_type, partition_count, current_partition)
                    SELECT 'roundrobin', %s, COUNT(*) %% %s FROM temp_numbered
                """, (numberofpartitions, numberofpartitions))

    openconnection.commit()
    cur.close()

    elapsed_time = time.time() - start_time
    print(f"roundrobinpartition executed in {elapsed_time:.2f} seconds")

