
import apache_beam as beam
import html
import emoji
import textstat
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions

pipeline_args = [
    '--project={}'.format('etl-python-poland-preparation'),
    '--runner={}'.format('DirectRunner'),
    '--temp_location=gs://dataflowtemporal-2/',
]

PROJECT_ID = 'etl-pycon-huge'
DATASET = 'pycon_data'
TABLE_NAME = 'pycon_clean_data'
SUBSCRIPTION_NAME = 'pycon-subscription'

_table_spec = f'{PROJECT_ID}:{DATASET}.{TABLE_NAME}'
_table_schema = 'clean_text:STRING, readability:NUMERIC'

subscription_id = f'projects/{PROJECT_ID}/subscriptions/{SUBSCRIPTION_NAME}'


def calculate_readability(content):
    """ Calculates the readability of a text according to the Flesch reading ease
    (lower numbers mark passages that are more difficult to read)
    https://en.wikipedia.org/wiki/Flesch%E2%80%93Kincaid_readability_tests

    Args:
        content: A dict with the cleaned up text

    Returns:
        The content with the added readability score
    """
    content['readability'] = textstat.flesch_reading_ease(content['clean_text'])
    return content


def convert_to_dict(content):
    """ Creates a dict from the pub sub message

    Args:
        content: The content we get from pub sub

    Returns:
        A dict with the content of the pub sub message as clean_text
    """
    content_dict = dict()
    content_dict['clean_text'] = content.data.decode('utf-8')
    return content_dict


def clean_up_text(content):
    """ Unescapes possibly encoded html from a dict with the clean_text attribute and removes emojis

    Args:
        content: The dict with the clean_text we want to clean

    Returns:
        A text free of emojis and unescaped html
    """
    aux_var = html.unescape(content['clean_text'])
    content['clean_text'] = remove_emoji(aux_var)
    return content


def remove_emoji(text):
    """Removes emojis from a text

    Taken from https://stackoverflow.com/questions/51784964/remove-emojis-from-multilingual-unicode-text/51785357#51785357

    Args:
        text: The text we want to clean up

    Returns:
        A text free of emojis
    """
    return emoji.get_emoji_regexp().sub(u'', text)


def filter_out_data(content):
    """Filters text from the dict that are too short for analyzing readability


    Args:
        content: The dict with the content we want to check

    Returns:
        None if the text is too short, dict itself if ok
    """
    if len(content['clean_text']) < 10:
        return None
    return content


def run():

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(StandardOptions).streaming = True

    pipeline = beam.Pipeline(options=pipeline_options)

    pipeline | 'Read from pub sub' >> beam.io.ReadFromPubSub(subscription=subscription_id,
                                                             with_attributes=True) | 'Convert to dictionary' >> beam.Map(
        convert_to_dict) | 'Filter out short entries' >> beam.Filter(filter_out_data) | 'Clean up text' >> beam.Map(
        clean_up_text) | 'Calculate readability' >> beam.Map(
        calculate_readability) | 'Write to big Query' >> beam.io.WriteToBigQuery(_table_spec, schema=_table_schema)

    pipeline.run().wait_until_finish()


if __name__ == '__main__':
    run()
