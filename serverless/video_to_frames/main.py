import ffmpeg
import s3fs
import tempfile
import os
from urllib.parse import unquote_plus


s3_fs = s3fs.S3FileSystem()


def handler(event, context):
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'])
        fd, filepath = tempfile.mkstemp()
        s3_fs.get(f's3://{bucket}/{key}', filepath)

        output_dir = tempfile.mkdtemp()
        video_name = os.path.splitext(key.split('/')[-1])[0]
        project_id = key.split('/')[1]

        # Example of video name {name}__{start}_{end}_{fps}.{extension}
        video_name, metadata = video_name.split('__')
        sec_start, sec_end, fps = [int(x) for x in metadata.split('_')]
        output_filename = f'{output_dir}/{video_name}_%d.png'

        try:
            ffmpeg.input(filepath) \
                .filter('trim', start=sec_start, end=sec_start) \
                .filter('fps', fps=fps) \
                .output(output_filename, video_bitrate='5000k', s='64x64', sws_flags='bilinear', start_number=0) \
                .run(capture_stdout=True, capture_stderr=True)

            s3_fs.put(output_dir, f's3://{bucket}/raw/{project_id}/', recursive=True)
        except ffmpeg.Error as e:
            print('stdout:', e.stdout.decode('utf8'))
            print('stderr:', e.stderr.decode('utf8'))
        finally:
            os.close(fd)


if __name__ == '__main__':
    handler(None, None)
