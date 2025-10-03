🌊 Flow
Flow is a declarative, type-safe programming language designed from the ground up for building robust, readable, and elegant data pipelines.

The Problem
Modern data engineering is often a messy patchwork of Python scripts, SQL queries, and YAML configurations held together by shell commands. This approach is powerful but makes pipelines difficult to visualize, hard to debug, and prone to runtime errors when data schemas change unexpectedly.

The Solution: Flow
Flow treats the data pipeline as a first-class citizen. Instead of writing imperative code to describe how to move and change data, you declaratively describe the flow of the data itself. The syntax is designed to be as intuitive as a whiteboard diagram, with built-in safety features to catch errors before they happen.

Key Principles
✨ Declarative & Visual Syntax: With a simple arrow operator (->), you define the path of your data. The code visually represents the data's journey, making it instantly understandable.

🔒 Type-Safe by Design: Define schema blocks for your data sources. Flow's validator will check your entire pipeline against these schemas before execution, eliminating a whole class of runtime errors.

🧩 Modular & Reusable: Use variables to store intermediate results. This allows you to break down complex workflows into logical, reusable pieces, just like in a traditional scripting language.

🔌 Extensible I/O: Flow is built to connect to the real world. Start with File() sources and sinks, and seamlessly integrate with live databases like Postgres(), with secure secret management via env().

Syntax at a Glance
Here’s what a complete Flow program looks like. It connects to a database, filters for senior users, creates a new column, sorts the results, selects the final columns, and saves the output to a new variable.

Code snippet

schema User {
  id: int;
  name: string;
  age: int;
  status: string;
}

source users_db <- Postgres(
  host: "localhost",
  password: env("DB_PASSWORD"),
  table: "users"
) using User;

// Create a new report by processing the database source
senior_report = users_db
    -> filter(user.status == 'active' and user.age > 50)
    -> mutate(report_name = "User: " + user.name)
    -> sort(age, order: 'desc')
    -> select(id, report_name);
Features
Core Language:

Variable Assignments (=)

Schema Declarations (schema)

Static Validation

Sources:

File(path: ...)

Postgres(host: ..., database: ..., ...)

Secure secret handling with env()

Sinks:

File(path: ...)

Transformations:

filter(): With complex boolean logic (and/or) and operators (>, <, ==).

select(): To choose a subset of columns.

sort(): To order data by multiple columns (asc/desc).

mutate(): To create new columns using arithmetic (+, -, *, /) and string concatenation.

Getting Started (for Developers)
This project is built with Python and Lark. To work on the Flow language itself:

Prerequisites: You'll need Python 3.9+, Git, and Docker (for the PostgreSQL database).

Clone the repository:

Bash

git clone <your-repo-url>
cd flow-language
Set up a virtual environment:

Bash

python -m venv venv
source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
Install dependencies:

Bash

pip install -r requirements.txt
Start the Database: Make sure Docker is running and start the PostgreSQL container:

Bash

docker run --name flow-postgres -e POSTGRES_PASSWORD=mysecretpassword -p 5432:5432 -d postgres
Running a Flow Script
The language is currently executed via the main transpiler script.

Set Environment Variables: If your script uses env(), make sure to set them in your terminal first.

Bash

export POSTGRES_PASSWORD='mysecretpassword'
Configure the Runner: Open src/main.py and change the test_file variable to point to the .flow script you want to run.

Execute:

Bash

python src/main.py
This will validate the script and print the generated Python code to the console.