import zstandard as zstd
import os
import json
import io


def zst_to_json(source_path, dest_path):
    """
    Reads all zst files from source_path by streaming each json element.
    The json element is then appended to a json file with the same name as the original archive.
    Streaming is needed bc the size of the compressed files is too large to be loaded in memory all at once.
    All data from a zst ends up in a json file.
    """
    for file in os.listdir(source_path):
        if '.zst' in file:
            # need to open as bytes
            with open(source_path + file, 'rb') as f:
                dctx = zstd.ZstdDecompressor(max_window_size=2147483648)
                stream_reader = dctx.stream_reader(f)
                text_stream = io.TextIOWrapper(stream_reader, encoding='utf-8')

                # if the json file already exists in the dest folder, delete it
                # this is because the appending might continue the previously generated files
                file_json = file.split('.')[0] + '.json'
                if file_json in os.listdir(dest_path):
                    os.remove(dest_path + file_json)

                with open(dest_path + file_json, 'a') as f_out:
                    for line in text_stream:
                        obj = json.loads(line)
                        json.dump(obj, f_out)
            print('Finished with ' + file)


path = 'D:\\MSc_DST\\Year1\\Q2\\BigData\\project\\data\\'
path_dest = 'D:\\MSc_DST\\Year1\\Q2\\BigData\\project\\data_json\\'
zst_to_json(path, path_dest)
