Python lambda example to convert audio files from an S3 bucket into text and detect sentiment using 

Amazon Transcribe 
Amazon Comprehend

And email the sentiment using SNS

# Description : 
# 1. Listens to S3 bucket put events 
# 2. Converts the audio file into text using transcribe service 
# 3. Detects the sentiment from the text using comprehend service
# 4. Sends email using SNS service
# 5. Save results to a DynamoDB table
