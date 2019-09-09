## Deploying an ETL with Python on Google Cloud Platform

The intention of this repo is to illustrate a simple ETL using python on GCP

The code is structured on numbered folders that keep on adding more complexity with each step, please review the code on each of the steps and try to understand it.

The first step would be cloning the repo and installing the dependencies:

`pip install -r requirements.txt`

Now go to https://console.cloud.google.com if you don’t have an account just click on try it for free, that free tier account is enough for following these exercise.

Create a new project, click on the project browser dropdown and click on ‘Create project’, give it a name and save it. Wait for the project to be created and then take note of the project id, we will need it later.

Go to 1.all_local and execute it with:

`python data_transformer.py `

This will read from the file test_data.txt and write the results of the transformation on a txt file.

* Go to the option IAM & admin.
* Click on service accounts.
* Click create service account.
* Fill in the name 'etl-service-account'.
* Select the role Dataflow admin.
* Select the role BigQuery Editor.
* Select the role Pub/Sub subscriber.
* Select the role bigquery editor.
* Click continue.
* Click on create key and download said key.
* Run (on a terminal): export GOOGLE_APPLICATION_CREDENTIALS=location/yourkey.json

We need this service account to run the application locally, and give it all the permissions we are going to need.

* Go to BigQuery, under the big data options.
* Click on Create a Data set, give it as a name 'pycon_data'.
* Click over the name of the newly created dataset.
* Click on Create table
* Click on Add field.
* Create a field of name 'clean_text' and type String.
* Click on add field name again.
* Create a field of name 'readability' and type Numeric.

We have a data set to write to now, let’s go to 2.local_to_bigquery and to data_transformer.py
replace the variable PROJECT_ID with your actual project id.

Run the project in that folder:

`python data_transformer.py`

You should see the resulting data on the BigQuery table we created.

* Go to cloud Pub/Sub, under the big data options.
* Click on create topic, put as name 'pyconpoland'.
* Click on Subscriptions.
* Click on Create subscription.
* The delivery type should be pull.
* The subscription name should be pycon-subscription
* The topic id should be 'projects/<project-id>/topics/datasubscription'.

Go to 3.local_from_pub_sub_to_bigquery, replace on data_transformer.py and replace the variable PROJECT_ID with your actual project id

Run it with:

`python data_transformer.py`

Leave it running, go to your Subscription on Pub/sub click on publish message, add some text and we should see the newly added line on BigQuery.

* Go to cloud functions, under the compute options.
* Click on create functions.
* Give it a name: 'publish_to_pub_sub'.
* Select python 3.7 as the run time.
* Copy the requirements from the sample code.
* Copy the main code on main.py.
* Function to execute should be publish_to_pub_sub.

Now we can go to cloud functions, click on test function and put a message like

`{"message": "This is a custom message from a cloud function"}`

We also could send a message hitting an URL like:

`https://<project-location>-<project-id>.cloudfunctions.net/publish_to_pub_sub?message=a message coming from a cloud function`

Replacing <project-region> and <project-id> with the correct values

* Go to Storage under the storage options.
* Click on create bucket. Enter a name for your bucket.
* Give it a name.
* Select regional as the location type.
* Choose the Standard storage class.

Go to the folder 5.final_version and run the code lie this:

`python data_transformer.py`

Wait for the script to stop.

Now let’s go to DataFlow under the Big Data options and we should see the data flow job, now we have our code running on the cloud!, we can send the data and see it reflected on BigQuery as we were doing with the cloud function.

You can find the presentation under the resources folder of this repo.
