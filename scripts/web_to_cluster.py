import os
import subprocess
import paramiko

if __name__ == '__main__':
    for type in [{"prefix": "RC", "type": "comments"}, {"prefix": "RS", "type": "submissions"}]:
        for i in range(1, 13):
            month = str(i) if i >= 10 else f"0{i}"
            file_no_ext = f"{type['prefix']}_2006-{month}"
            file_zst = f"{file_no_ext}.zst"
            file_json = f"{file_no_ext}.json"
            link = f"https://files.pushshift.io/reddit/{type['type']}/{file_zst}"
            command = f"wget {link} -outfile {file_zst}"
            print(command)
            powershell_command = f'C:/Windows/System32/WindowsPowerShell/v1.0/powershell.exe {command}'
            process = subprocess.Popen(powershell_command, shell=True)
            return_code = process.wait()
            print(return_code)
            command = f"zstd -d {file_zst} --long=31 -o {file_json}"
            process = subprocess.Popen(command, shell=True)
            return_code = process.wait()
            print(return_code)
            with open("password.txt", "rt") as f:
                pwd = f.read()
            command = f"pscp -P 22 -pw {pwd} .\\{file_json} s3049221@ctit022.ewi.utwente.nl:"
            powershell_command = f'C:/Windows/System32/WindowsPowerShell/v1.0/powershell.exe {command}'
            process = subprocess.Popen(command, shell=True)
            return_code = process.wait()
            print(return_code)
            os.remove(file_zst)
            os.remove(file_json)
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect("ctit022.ewi.utwente.nl", username="s3049221", password=pwd)
            ssh.exec_command(f"hdfs dfs -copyFromLocal /home/s3049221/{file_json} /user/s3049221/reddit/{file_json}")