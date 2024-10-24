import io
import os
import psycopg2
import rasterio
from rasterio.io import MemoryFile
from rasterio.transform import Affine
import time
import huffman
import numpy as np
import random
import collections
import pickle

# Kết nối đến cơ sở dữ liệu PostgreSQL
conn = psycopg2.connect(
    host="localhost",
    database="postgres",
    user="postgres",
    password="trong1515"
)

cur = conn.cursor()

switch = {
    1: "Huffman",
    2: "JPEG2000",
    3: "RLE"
}


def Check_to_compress(directory, option_compress):
    Folder_Name = os.path.basename(directory)
    cur.execute("SELECT COUNT(*) FROM folder_compress WHERE namefl=%s AND type=%s",
                (Folder_Name, "Huffman" if option_compress == 1 else "JPEG2000" if option_compress == 2 else "RLE"))
    result = cur.fetchone()
    if result and result[0] == 1:
        print("Tệp " + Folder_Name + " này đã được nén!")
    else:
        Id = random.randint(1, 1000)
        while True:
            cur.execute("SELECT COUNT(*) FROM folder_compress WHERE idfl=%s", (Id,))
            Count = cur.fetchone()
            if Count and Count[0] == 1:
                Id = random.randint(1, 1000)
            else:
                break
        if option_compress == 2:
            cur.execute("INSERT INTO folder_compress (idfl, namefl, type) VALUES (%s, %s, %s)",
                        (Id, Folder_Name, "JPEG2000"))
        elif option_compress == 3:
            cur.execute("INSERT INTO folder_compress (idfl, namefl, type) VALUES (%s, %s, %s)",
                        (Id, Folder_Name, "RLE"))
        else:
            cur.execute("INSERT INTO folder_compress (idfl, namefl, type) VALUES (%s, %s, %s)",
                        (Id, Folder_Name, "Huffman"))
        conn.commit()
        Read_File(directory, Id, option_compress)


def Check_to_decompress(output_folder, option_compress, name_folder_compress):
    if option_compress == 2:
        DecompressJPEG2000(output_folder, name_folder_compress)
    elif option_compress == 3:
        DecompressRLE(output_folder, name_folder_compress)



def Read_File(directory, id, option_compress):
    for filename in os.listdir(directory):
        if os.path.isfile(os.path.join(directory, filename)):
            if filename.endswith('.TIF'):
                if option_compress == 1:
                    CompressHuffman(directory, filename, id)
                elif option_compress == 2:
                    CompressJPEG2000(directory, filename, id)
                else:
                    CompressRLE(directory, filename, id)
            else:
                with open(os.path.join(directory, filename), "rb") as FL:
                    start_time = time.time()
                    OtherFile = FL.read()
                    Save_Database(OtherFile, filename, len(OtherFile) / (1024 ** 2), id, start_time, 0,
                                  0, 0, "", 0, 0, 0,
                                  0, 0, 0, 0, 0, 0)

def CompressHuffman(directory, filename, id):
    with rasterio.open(os.path.join(directory, filename)) as src:
        start_time = time.time()
        Data_raster = src.read(1)
        Data_raster_byte = Data_raster.tobytes()
        byte_frequencies = collections.Counter(Data_raster_byte)
        Codebook = huffman.codebook(byte_frequencies.items())
        Compressed_data = ''.join(Codebook[byte] for byte in Data_raster_byte)
        with io.BytesIO() as MemoryFile:
            pickle.dump(Codebook, MemoryFile)
            MemoryFile.seek(0)
            print("Name: {:s}, Time: {:.2f}s".format(filename, time.time() - start_time))
            print("Capacity file: {:.2f} MB".format(len(Compressed_data) / (1024 ** 2)))
            print("Capacity tree: {:f} MB".format(MemoryFile.getbuffer().nbytes / (1024 ** 2)))


def CompressJPEG2000(directory, filename, id):
    new_filename = os.path.splitext(filename)[0] + ".jp2"
    if os.path.exists(os.path.join(directory, new_filename)):
        print(f"File {new_filename} đã tồn tại và sẽ bị bỏ qua.")
        return

    with rasterio.open(os.path.join(directory, filename)) as src:
        start_time = time.time()
        # Sao chép toàn bộ profile từ file nguồn
        profile = src.profile.copy()
        
        # Đảm bảo giữ nguyên các thông tin địa lý
        profile.update(
            driver='JP2OpenJPEG',
            compress='jpeg2000',
            quality=25,
            tile_width=512,
            tile_height=512,
            # Thêm các thông tin tham chiếu địa lý
            crs=src.crs,
            transform=src.transform,
            nodata=src.nodata
        )
        
        data = src.read()
        print(f"Đang nén tệp: {filename}")
        print(f"Số lượng band: {data.shape[0]}")
        print(f"Kích thước dữ liệu trước khi nén: {data.nbytes} bytes")
        
        # Kiểm tra và in thông tin tham chiếu địa lý
        print(f"CRS: {profile['crs']}")
        print(f"Transform: {profile['transform']}")
        
        with MemoryFile() as memfile:
            with rasterio.open(memfile, 'w', **profile) as Compressed:
                Compressed.write(data)
                # Đảm bảo các tags metadata được sao chép
                Compressed.update_tags(**src.tags())
                # Sao chép metadata của từng band nếu có
                for i in range(1, src.count + 1):
                    Compressed.update_tags(i, **src.tags(i))
            
            memfile.seek(0)
            File_Compressed = memfile.read()
            
            Save_Database(File_Compressed, new_filename, len(File_Compressed) / (1024 ** 2),
                          id, start_time, src.height, src.width, src.count, src.dtypes[0], src.transform[0],
                          src.transform[1], src.transform[2], src.transform[3], src.transform[4], src.transform[5],
                          src.transform[6], src.transform[7], src.transform[8])
            
            print(f"Đã nén và lưu trữ thành công tệp: {new_filename}")
            print(f"Thời gian nén: {time.time() - start_time:.2f}s")
            print(f"Dung lượng sau khi nén: {len(File_Compressed) / (1024 ** 2):.2f} MB")

def DecompressJPEG2000(output_folder, name_folder_compress):
    cur.execute("SELECT * FROM folder_compress WHERE namefl=%s AND type=%s", (name_folder_compress, "JPEG2000"))
    data_folder = cur.fetchone()
    new_path = os.path.join(output_folder, data_folder[1])
    if not os.path.exists(new_path):
        os.makedirs(new_path)
    else:
        print(f"Thư mục {new_path} đã tồn tại và sẽ bị ghi đè.")
        
    cur.execute(
        "SELECT namef,dataf,height,width,band,dtype,transform1,transform2,transform3,transform4,transform5,transform6"
        " FROM file_compress WHERE idfl=%s", (data_folder[0],))
    data_files_compress = cur.fetchall()
    for files_compress in data_files_compress:
        start_time = time.time()
        if files_compress[0].endswith(".jp2"):
            try:
                with io.BytesIO(files_compress[1]) as memfile:
                    with rasterio.open(memfile) as src:
                        print(f"Đang giải nén tệp: {files_compress[0]}")
                        print(f"Số lượng band trong tệp nén: {src.count}")
                        # Tạo transform từ các thành phần được lưu trong database
                        transform = Affine(
                            files_compress[6], files_compress[7], files_compress[8],
                            files_compress[9], files_compress[10], files_compress[11]
                        )
                        
                        # Sao chép profile và cập nhật transform
                        profile = src.profile.copy()
                        profile.update(
                            driver='GTiff',
                            transform=transform
                        )
                        
                        data = src.read()
                        print(f"Số lượng band sau khi đọc: {data.shape[0]}")
                        new_name_file = os.path.splitext(files_compress[0])[0] + ".TIF"
                        output_file_path = os.path.join(new_path, new_name_file)
                        
                        if os.path.exists(output_file_path):
                            print(f"File {output_file_path} đã tồn tại và sẽ bị ghi đè.")
                            
                        with rasterio.open(output_file_path, "w", **profile) as File_decompress:
                            File_decompress.write(data)
                            print(f"Số lượng band trước khi ghi: {data.shape[0]}")
                            
                        print("Giải nén thành công file: {:s}, Trong vòng: {:.2f}s".format(
                            new_name_file, time.time() - start_time))                            
            except Exception as e:
                print(f"Lỗi khi giải nén và lưu trữ tệp {files_compress[0]}: {e}")

def CompressRLE(directory, filename, id):
    with rasterio.open(os.path.join(directory, filename)) as src:
        start_time = time.time()
        data = src.read(1)
        data_flat = data.flatten()
        compressed_data = []
        count = 1
        for i in range(1, len(data_flat)):
            if data_flat[i] == data_flat[i - 1]:
                count += 1
            else:
                compressed_data.append((data_flat[i - 1], count))
                count = 1
        compressed_data.append((data_flat[-1], count))
        compressed_data_bytes = pickle.dumps(compressed_data)
        Save_Database(compressed_data_bytes, filename, len(compressed_data_bytes) / (1024 ** 2), id, start_time,
                      src.height, src.width, src.count, src.dtypes[0], src.transform[0], src.transform[1],
                      src.transform[2], src.transform[3], src.transform[4], src.transform[5], src.transform[6],
                      src.transform[7], src.transform[8])
def DecompressRLE(output_folder, name_folder_compress):
    cur.execute("SELECT * FROM folder_compress WHERE namefl=%s AND type=%s", (name_folder_compress, "RLE"))
    data_folder = cur.fetchone()
    new_path = os.path.join(output_folder, data_folder[1])
    if not os.path.exists(new_path):
        os.makedirs(new_path)
    else:
        print(f"Thư mục {new_path} đã tồn tại và sẽ bị ghi đè.")
        
    cur.execute(
        "SELECT namef,dataf,height,width,band,dtype,transform1,transform2,transform3,transform4,transform5,transform6"
        " FROM file_compress WHERE idfl=%s", (data_folder[0],))
    data_files_compress = cur.fetchall()
    for files_compress in data_files_compress:
        start_time = time.time()
        try:
            compressed_data = pickle.loads(files_compress[1])
            data_flat = []
            for value, count in compressed_data:
                data_flat.extend([value] * count)
            data = np.array(data_flat, dtype=files_compress[5]).reshape((files_compress[2], files_compress[3]))
            transform = Affine(
                files_compress[6], files_compress[7], files_compress[8],
                files_compress[9], files_compress[10], files_compress[11]
            )
            profile = {
                'driver': 'GTiff',
                'height': files_compress[2],
                'width': files_compress[3],
                'count': files_compress[4],
                'dtype': files_compress[5],
                'transform': transform
            }
            new_name_file = os.path.splitext(files_compress[0])[0] + ".TIF"
            output_file_path = os.path.join(new_path, new_name_file)
            if os.path.exists(output_file_path):
                print(f"File {output_file_path} đã tồn tại và sẽ bị ghi đè.")
            with rasterio.open(output_file_path, "w", **profile) as File_decompress:
                File_decompress.write(data, 1)
            print(f"Giải nén thành công file: {new_name_file}, Trong vòng: {time.time() - start_time:.2f}s")
        except Exception as e:
            print(f"Lỗi khi giải nén và lưu trữ tệp {files_compress[0]}: {e}")
    print(
        "----------------------------------------------------\nToàn bộ file đã giải nén đang ở thư mục: {:s}\nTại "
        "vị trí: {:s}".format(data_folder[1], output_folder))

def Save_Database(File, Name, Capacity_file, id, start_time, height, width, band, dtype,
                  transform_1, transform_2, transform_3, transform_4,
                  transform_5, transform_6, transform_7, transform_8, transform_9):
    cur.execute("INSERT INTO file_compress (idfl, namef, dataf, megabyte, time, height, width, band, dtype, "
                "transform1, transform2, transform3, transform4, transform5, transform6, "
                "transform7, transform8, transform9) VALUES "
                "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (id, Name, File, Capacity_file, time.time() - start_time, height, width, band, dtype,
                 transform_1, transform_2, transform_3, transform_4,
                 transform_5, transform_6, transform_7, transform_8, transform_9))
    conn.commit()
    print("Đã nén và lưu trữ thành công tệp: {:s}, Trong vòng: {:.2f}s, Dung lượng: {:.2f} MB".format(Name,time.time() - start_time, len(File) / (1024 ** 2)))

directory = "D:/Học Hành Coding/PPNCKH/Code/LE07_L2SP_124053_20221229_20230124_02_T1"
output_folder = "D:/Học Hành Coding/PPNCKH/Code/Output"

def Menu():
    print("------------------------------------------\nNén và giải nén ảnh viễn thám\n------------------------------------------\nOption\n1 - Để nén dữ liệu\n2 - Để giải nén dữ liệu\n------------------------------------------")
    Option = int(input("Nhập: "))
    if Option == 1:
        print(
            "------------------------------------------\nNén ảnh viễn thám\n------------------------------------------")
        Folder = str(input("Nhập đường dẩn thư mục ảnh vệ tinh: "))
        Folder = Folder.replace("\\", "/")
        if os.path.exists(Folder) and os.path.isdir(Folder):
            files = os.listdir(Folder)
            RequireFile = [f for f in files if f.endswith('.TIF')]
            if RequireFile:
                print("------------------------------------------\nOption\n1 - Huffmam Code ( Bản thu nhận kết quả )\n2 - JPEG2000\n3 - RLE\n------------------------------------------")
                Option = int(input("Nhập: "))
                Check_to_compress(Folder, Option)
            else:
                print("Đây không phải là thư mục hình ảnh vệ tinh!")
                Menu()
        else:
            print("Thư mục không tồn tại!")
            Menu()
    elif Option == 2:
        print(
            "------------------------------------------\nGiải nén ảnh viễn thám\n------------------------------------------")
        Folder = str(input("Nhập đường dẩn nơi sẽ lưu trữ: "))
        if os.path.exists(Folder) and os.path.isdir(Folder):
            cur.execute("SELECT * FROM folder_compress")
            Folder_Compressed = cur.fetchall()
            if not Folder_Compressed:
                print("Chưa có thư mục nào hiện đã nén!")
                Menu()
            else:
                print(f"{'ID':<6} {'Tên ảnh':<45} {'Dạng nén':<20}")
                for FC in Folder_Compressed:
                    print("{:<6} {:<45} {:<20}".format(FC[0], FC[1], FC[2]))
                print("------------------------------------------")
                Option = int(input("Nhập ID để chọn thư mục cần giải nén:"))
                cur.execute("SELECT * FROM folder_compress WHERE idfl=%s", (Option,))
                Require_Folder = cur.fetchone()
                if not Require_Folder:
                    print("Không có thư mục nào có ID là " + str(Option))
                    Menu()
                else:
                    Check_to_decompress(Folder, 2 if Require_Folder[2] == "JPEG2000" else 3 if Require_Folder[2] == "RLE" else 1,
                                        Require_Folder[1])

        else:
            print("Thư mục không tồn tại!")
            Menu()
    else:
        print("Shut Down!")


Menu()
cur.close()
conn.close()