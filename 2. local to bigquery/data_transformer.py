
import apache_beam as beam
import html
import textstat
import emoji

PROJECT_ID = 'etl-pycon-huge'
DATASET = 'pycon_data'
TABLE_NAME = 'pycon_clean_data'

_table_spec = f'{PROJECT_ID}:{DATASET}.{TABLE_NAME}'
_table_schema = 'clean_text:STRING, readability:NUMERIC'


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


def convert_to_dict(text):
    """ Unescapes possibly encoded html from a text and removes emojis

    Args:
        text: The previously cleaned up text

    Returns:
        A dict with the cleaned up text as clean_text
    """
    content_dict = dict()
    content_dict['clean_text'] = text
    return content_dict


def clean_up_text(text):
    """ Unescapes possibly encoded html from a text and removes emojis

    Args:
        text: The text we want to clean up

    Returns:
        A text free of emojis and unescaped html
    """
    text = html.unescape(text)
    return remove_emoji(text)


def remove_emoji(text):
    """Removes emojis from a text

    Taken from https://stackoverflow.com/questions/51784964/remove-emojis-from-multilingual-unicode-text/51785357#51785357

    Args:
        text: The text we want to clean up

    Returns:
        A text free of emojis
    """
    return emoji.get_emoji_regexp().sub(u'', text)


def filter_out_data(text):
    """Filters text that is too short for analyzing readability


    Args:
        text: The text we want to check for proper length

    Returns:
        None if the text is too short, the text itself if it has the correct length
    """
    if len(text) < 30:
        return None
    return text


def run():

    pipeline = beam.Pipeline('DirectRunner')

    pipeline | 'Read data from txt' >> beam.io.ReadFromText(
        'test_data.txt') | 'Filter out short entries' >> beam.Filter(filter_out_data) | 'Clean up text' >> beam.Map(
        clean_up_text) | 'Convert to a dictionary' >> beam.Map(convert_to_dict) | 'Calculate readability' >> beam.Map(
        calculate_readability) | 'Write to big Query' >> beam.io.WriteToBigQuery(_table_spec, schema=_table_schema)
    pipeline.run()


if __name__ == '__main__':
    run()
