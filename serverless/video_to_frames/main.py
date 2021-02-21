import ffmpeg
import s3fs
import tempfile
from urllib.parse import unquote_plus


s3_fs = s3fs.S3FileSystem()


def handler(event, context):
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'])
        fd, filepath = tempfile.mkstemp()
        s3_fs.get(f's3://{bucket}/{key}', filepath)

        output_dir = tempfile.mkdtemp()
        video_name = key.split('/')[-1]

        try:
            ffmpeg.input(filepath) \
                .filter('fps', fps=2) \
                .output(f'{output_dir}/{video_name}_%d.png',
                        video_bitrate='5000k',
                        s='64x64',
                        sws_flags='bilinear',
                        start_number=0) \
                .run(capture_stdout=True, capture_stderr=True)
        except ffmpeg.Error as e:
            print('stdout:', e.stdout.decode('utf8'))
            print('stderr:', e.stderr.decode('utf8'))


if __name__ == '__main__':
    handler(None, None)
