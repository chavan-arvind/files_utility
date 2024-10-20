#!/bin/bash

# Make sure Homebrew is installed
if ! command -v brew &> /dev/null; then
    echo "Installing Homebrew..."
    /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
else
    echo "Homebrew already installed"
fi

# Install MySQL if not already installed
if ! command -v mysql &> /dev/null; then
    echo "Installing MySQL..."
    brew install mysql
else
    echo "MySQL already installed"
fi

# Start MySQL service
echo "Starting MySQL service..."
brew services start mysql

# Create Python virtual environment
echo "Setting up Python virtual environment..."
python3 -m venv venv
source venv/bin/activate

# Install requirements
echo "Installing Python dependencies..."
pip install -r requirements.txt

echo "Setup complete!"
echo "Now you should:"
echo "1. Run: mysql_secure_installation"
echo "2. Follow the prompts to set up your root password"
echo "3. Update the mysql_config password in the Python script"
echo "4. Run the Python script: python process_data.py"