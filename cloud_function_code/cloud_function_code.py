from google.cloud import pubsub_v1

publisher = pubsub_v1.PublisherClient()
topic_name = 'pyconpoland'
project_id = 'etl-python-poland-preparation'
topic_path = publisher.topic_path(project_id, topic_name)


def publish_to_pub_sub(request):
    request_json = request.get_json()
    message = 'Default message to pub sub'
    if request.args and 'message' in request.args:
        message = request.args.get('message')
    elif request_json and 'message' in request_json:
        message = request_json['message']
    publisher.publish(topic_path, str.encode(message))
