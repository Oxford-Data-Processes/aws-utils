FROM public.ecr.aws/lambda/python:3.11

RUN yum install -y git

COPY requirements.txt ./

RUN pip install -r requirements.txt

COPY . ./

CMD ["main.lambda_handler"]