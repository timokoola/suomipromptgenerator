

# deploy the cloud function to gcp
# set env variable for bucket name
# runtime is set to python311
# trigger cron job to run every day at 12:00
deploy:
	gcloud functions deploy suomipromptgenerator \
	--gen2 \
	--region=europe-north1 \
	--runtime=python311 \
	--source=. \
	--entry-point=generate_prompt

list-files:
	gcloud meta list-files-for-upload
	 