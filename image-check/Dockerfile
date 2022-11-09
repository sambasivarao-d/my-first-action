# Python image to use.
FROM python:3.9
# Set the working directory to /SPUR_Masterdata_API_Certificates_hapitobq
WORKDIR /cr-spur-generic-code
# copy the requirements file used for dependencies
COPY requirements.txt .
# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
# Copy the rest of the working directory contents into the container at /SPUR_Masterdata_API_Certificates_hapitobq
COPY . .
CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 spur_hapi_to_bq_generic_code:app