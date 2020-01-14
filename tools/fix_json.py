import boto3
from datetime import date

def fix_json(location, day):
    filename = location + day

    f = open(filename, 'r')
    contents = f.readlines()
    f.close()

    contents.insert(0, "{\""+day+"\":")
    contents.append("}")
    f = open(filename, 'w')
    f.writelines(contents)
    f.close()

def fix_files():
    s3 = boto3.client('s3')

    location = '/tmp/'

    for i in range(20200106, 20200114):
        s3.download_file('andrei-housing-prices', str(i), location+str(i))
        fix_json(location, str(i))

        data = open(location+str(i), 'rb')
        s3.put_object(Bucket='andrei-housing-prices', Key=str(i), Body=data)

if __name__ == "__main__":
    fix_files()
