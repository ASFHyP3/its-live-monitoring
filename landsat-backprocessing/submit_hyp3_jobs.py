from hyp3_sdk import HyP3
from hyp3_sdk.util import chunk

PUBLISH_BUCKET = 'its-live-data'

hyp3 = HyP3(api_url='https://hyp3-its-live.asf.alaska.edu', username='hyp3.its_live')

with open('deduplicated_pairs.csv') as f:
    lines = f.read().strip('\n').split('\n')

pairs = [line.split(',') for line in lines]

prepared_jobs = [
    {
        'job_parameters': {
            'granules': [reference, secondary],
            'publish_bucket': PUBLISH_BUCKET,
        },
        'job_type': 'AUTORIFT',
        'name': reference,
    }
    for secondary, reference in pairs
]

with open('submitted_jobs.csv', 'w') as submitted_jobs_csv, \
        open('failed_submissions.csv', 'w') as failed_submissions_csv:
    for job_batch in chunk(prepared_jobs):
        try:
            submitted_jobs = hyp3.submit_prepared_jobs(job_batch)
        except Exception as e:
            failed_submissions_csv.write(
                '\n'.join(
                    ','.join([job['job_parameters']['granules'][1], job['job_parameters']['granules'][0]])
                    for job in job_batch
                ) + '\n'
            )
            print(e)
            continue
        submitted_jobs_csv.write(
            '\n'.join(
                ','.join([job.job_parameters['granules'][1], job.job_parameters['granules'][0], job.job_id])
                for job in submitted_jobs
            ) + '\n'
        )
