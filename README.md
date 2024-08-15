# GCS_to_Snowflake_WithAirflow
This project sets up an Apache Airflow workflow running on Docker to automate the ETL process from Google Cloud Storage (GCS) to Snowflake. Once the data is successfully loaded into Snowflake, a notification is sent via Gmail.

## Architecture

  ![GCS_to_Snowflake](https://github.com/user-attachments/assets/065048a6-442a-4b8d-b845-122f10d797e5)

## Technologies
- Python
- Docker
- Apache Airflow
- Snowflake
- Gmail

## Starting Airflow
Use Docker Compose to start Airflow and its dependencies:
```yaml
docker-compose up -d --build
```

To stop the Docker containers, use:
```yaml
docker-compose down
```
## Running the Pipeline
Once Docker Compose is running, you can access the Airflow web interface at **http://localhost:8080**.

Log in with the default credentials:

**Username**: airflow

**Password**: airflow

## Connect Gmail with Apache Airflow

### 1. Enable "Less Secure App Access" in Your Gmail Account
- **Sign** in to your Gmail account at [Google Account](https://myaccount.google.com/). 
- Go to the Security section.
- Enable **Less secure app access** to allow connections from Airflow via SMTP.

  **Note:** Enabling "Less secure app access" may be less secure. If you have 2-Step Verification enabled, consider using "App Passwords" instead.

### 2. Create an App Password (if using 2-Step Verification)
- **Sign in** to your Google account.
- Go to Security and select **App passwords**.
- Generate an app password for Airflow (select "Mail" and "Other" to get a password that can be used with Airflow).

  ![Screenshot 2024-03-05 020731](https://github.com/user-attachments/assets/0d0c0b3a-b991-4db4-a525-6a7541dfa26a)


### 3. Configure SMTP Settings in Airflow
- Open the Airflow configuration file **airflow.cfg**.

### 4. Create a New Connection
- Go to the **Admi**n tab and select **Connections**.
- Click on the + button to create a new connection.

### 5. Configure Connection
Fill in the details for the connection:
- **Conn Id:** smtp_default
- **Conn Type:** Email
- **Host:** smtp.gmail.com
- **Schema:** Leave blank
- **Login:** your-gmail.com
- **Password:** your-app-password

## Create IAM
### 1. Create a New Bucket
- **Name Your Bucket:**stock_market_us
- **Regional:** asia-southeast1

### 2. Navigate to IAM & Admin:
- Click on **IAM & Admin** in the left-hand menu.

### 3. Go to Service Accounts:
- In the IAM & Admin section, click on **Service accounts**.

### 4. Create a Service Account:
- Click on **Create Service Account** at the top of the page.
- Provide a name and description for the service account.
- Click **Create**.

### 5. Assign Roles:
- In the next screen, select the roles you want to assign to the service account. 

![Screenshot 2024-03-05 010055](https://github.com/user-attachments/assets/b5fecfd8-1b66-496e-ab54-88b40d535318)

- Click Continue.

### 6. Create a Key:
- In the next step, click **Create Key**.
- Select **JSON** and click **Create**.
- A **JSON** key file will be downloaded.

### 7. Create a Custom IAM Role for Snowflake

![Screenshot 2024-03-05 010617](https://github.com/user-attachments/assets/b7019f84-f04a-426d-9a71-3f6ccac924db)

### 8. Configure Snowflake to Access the GCS Bucket
- Navigate to **Storage** > **Browser** in the GCP console.
- Select the bucket you created (**stock_market_us**).
- Click on the **Permissions** tab.

![Screenshot 2024-03-05 011739](https://github.com/user-attachments/assets/38702fa1-2b52-497d-ab2a-314d3fae0737)

## Airflow Connection to Snowflake

![Screenshot 2024-03-05 111532](https://github.com/user-attachments/assets/41f0cdd1-0345-49ec-b850-61afd0c1189c)


![Screenshot 2024-03-05 111546](https://github.com/user-attachments/assets/8b79925a-415f-4fa1-b21f-cba3fcd2cb0a)

## Data Pipeline
Completed Data Pipeline

![Screenshot 2024-03-05 110302](https://github.com/user-attachments/assets/0400fa42-a548-4537-b97e-367cb05aa2d5)

![Screenshot 2024-03-05 110348](https://github.com/user-attachments/assets/811c40a0-ba88-4710-baf8-46394e99990c)

## References
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Running Airflow in Docker](https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html)
- [Snowflake Documentation](https://docs.snowflake.com/)
- [Google Cloud Storage Documentation](https://cloud.google.com/storage/docs)
- [Google Cloud Identity and Access Management Documentation](https://cloud.google.com/iam/docs)





