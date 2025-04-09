#!/bin/bash
# Script to set up Google Cloud Pub/Sub for tests

echo "=== Google Cloud Pub/Sub Setup ==="
echo "This script will help you set up Google Cloud credentials for Pub/Sub testing."

# Step 1: Authenticate with Google Cloud
echo
echo "Step 1: Authenticating with Google Cloud"
echo "-----------------------------------------"
echo "You need to authenticate with Google Cloud to use Pub/Sub services."
echo

echo "Authentication options:"
echo "1) Open browser for authentication (default)"
echo "2) Generate login URL (copy/paste to your preferred browser)"
echo "3) Skip authentication (if already authenticated)"
echo "Choose option (1-3): "
read AUTH_OPTION

if [ "$AUTH_OPTION" = "1" ]; then
    echo "Running default browser authentication..."
    gcloud auth login
elif [ "$AUTH_OPTION" = "2" ]; then
    echo "Generating authentication URL..."
    URL=$(gcloud auth login --no-launch-browser --brief)
    echo "Please open this URL in your preferred browser:"
    echo "$URL"
    echo "After authenticating, copy the verification code and paste it here:"
    read CODE
    gcloud auth login --no-launch-browser --code="$CODE"
elif [ "$AUTH_OPTION" = "3" ]; then
    echo "Skipping authentication..."
else
    echo "Invalid option. Defaulting to option 1."
    gcloud auth login
fi

echo
echo "Application Default Credentials options:"
echo "1) Open browser for ADC setup (default)"
echo "2) Generate login URL (copy/paste to your preferred browser)"
echo "3) Skip ADC setup (if already set up)"
echo "Choose option (1-3): "
read ADC_OPTION

if [ "$ADC_OPTION" = "1" ]; then
    echo "Setting up application default credentials (browser)..."
    gcloud auth application-default login
elif [ "$ADC_OPTION" = "2" ]; then
    echo "Generating ADC authentication URL..."
    URL=$(gcloud auth application-default login --no-launch-browser)
    echo "Please open this URL in your preferred browser:"
    echo "$URL"
    echo "After authenticating, copy the verification code and paste it here:"
    read CODE
    gcloud auth application-default login --no-launch-browser --code="$CODE"
elif [ "$ADC_OPTION" = "3" ]; then
    echo "Skipping ADC setup..."
else
    echo "Invalid option. Defaulting to option 1."
    gcloud auth application-default login
fi

# Step 2: Set the project
echo
echo "Step 2: Setting up your project"
echo "-------------------------------"
echo "Enter your Google Cloud project ID: "
read PROJECT_ID

if [ -z "$PROJECT_ID" ]; then
  echo "Project ID cannot be empty. Exiting."
  exit 1
fi

echo "Setting project to $PROJECT_ID..."
gcloud config set project $PROJECT_ID

# Step 3: Enable Pub/Sub API
echo
echo "Step 3: Enabling Pub/Sub API"
echo "----------------------------"
echo "Enabling Pub/Sub API for project $PROJECT_ID..."
gcloud services enable pubsub.googleapis.com --project=$PROJECT_ID

# Step 4: Configure the test environment file
echo
echo "Step 4: Configuring test environment file"
echo "----------------------------------------"
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/../../../.." &> /dev/null && pwd )"
ENV_FILE="$PROJECT_ROOT/tests/.env.test"

# Create the tests directory if it doesn't exist
mkdir -p "$(dirname "$ENV_FILE")"

# Check if the file exists
if [ -f "$ENV_FILE" ]; then
  echo "Found test environment file at $ENV_FILE"
else
  echo "Creating new test environment file at $ENV_FILE"
  touch "$ENV_FILE"
fi

# Check if the GCP_PROJECT_ID is already in the file
if grep -q "GCP_PROJECT_ID" "$ENV_FILE"; then
  # Update the existing value
  sed -i "s/GCP_PROJECT_ID=.*/GCP_PROJECT_ID=$PROJECT_ID/" "$ENV_FILE"
  echo "Updated GCP_PROJECT_ID in $ENV_FILE"
else
  # Add the new value
  echo "GCP_PROJECT_ID=$PROJECT_ID" >> "$ENV_FILE"
  echo "Added GCP_PROJECT_ID to $ENV_FILE"
fi

# Step 5: Create topics and subscriptions
echo
echo "Step 5: Creating Pub/Sub topics and subscriptions"
echo "------------------------------------------------"
echo "Creating required topics and subscriptions for test sites..."

# Define the test sites and their topics
SITES=("fidelity" "etrade" "robinhood")

for SITE in "${SITES[@]}"; do
  TOPIC="${SITE}-stock-trades"
  SUBSCRIPTION="${TOPIC}-sub"
  
  # Check if the variable exists in the env file
  if ! grep -q "PUBSUB_TOPIC_${SITE^^}" "$ENV_FILE"; then
    echo "PUBSUB_TOPIC_${SITE^^}=$TOPIC" >> "$ENV_FILE"
    echo "Added PUBSUB_TOPIC_${SITE^^} to $ENV_FILE"
  fi
  
  # Create the topic if it doesn't exist
  if gcloud pubsub topics describe $TOPIC --project=$PROJECT_ID 2>/dev/null; then
    echo "Topic $TOPIC already exists"
  else
    echo "Creating topic $TOPIC..."
    gcloud pubsub topics create $TOPIC --project=$PROJECT_ID
  fi
  
  # Create the subscription if it doesn't exist
  if gcloud pubsub subscriptions describe $SUBSCRIPTION --project=$PROJECT_ID 2>/dev/null; then
    echo "Subscription $SUBSCRIPTION already exists"
  else
    echo "Creating subscription $SUBSCRIPTION for topic $TOPIC..."
    gcloud pubsub subscriptions create $SUBSCRIPTION --topic=$TOPIC --project=$PROJECT_ID
  fi
done

if ! grep -q "TEST_TRADING_SITES" "$ENV_FILE"; then
  echo "TEST_TRADING_SITES=fidelity,etrade,robinhood" >> "$ENV_FILE"
  echo "Added TEST_TRADING_SITES to $ENV_FILE"
fi

echo
echo "=== Setup Complete ==="
echo "Your Google Cloud Pub/Sub environment is now set up for testing."
echo "You can run tests with cloud Pub/Sub using:"
echo "./run_online_test.sh $PROJECT_ID 5 30 true"
echo
echo "To check your topics and subscriptions, run:"
echo "gcloud pubsub topics list --project=$PROJECT_ID"
echo "gcloud pubsub subscriptions list --project=$PROJECT_ID" 