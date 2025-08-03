<a id="readme-top"></a>

# SQL-Exchange: Transforming SQL Queries Across Domains

This repository contains the artifacts for our paper: 

**SQL-Exchange: Transforming SQL Queries Across Domains**

---

<!-- TABLE OF CONTENTS -->
<!-- <details>
  <summary>Table of Contents</summary>
  <ol>
    <li><a href="#🔍-overview">Overview</a></li>
    <li>
      <a href="#📁-repository-structure">Repository Structure</a>
      <ul>
        <li><a href="#top-level-structure">Top Level Structure</a></li>
        <li><a href="#dataset-structure">Dataset Structure</a></li>
        <li><a href="#output-structure">Output Structure</a></li>
      </ul>
    </li>
    <li>
      <a href="#📋-data-structure">Data Structure</a>
      <ul>
        <li><a href="#questions-json">Questions Json</a></li>
        <li><a href="#samples-json">Samples Json</a></li>
        <li><a href="#schemas-json">Schemas Json</a></li>
      </ul>
    </li>
    <li>
      <a href="#🧪-reproducing-results">Reproducing Results</a>
      <ul>
        <li><a href="#prerequisites">Prerequisites</a></li>
        <li><a href="#installation">Installation</a></li>
      </ul>
    </li>
  </ol>
</details> -->

## 🔍 Overview

SQL-Exchange explores the problem of mapping SQL queries from a source database to a structurally consistent version over a target schema, using large language models (LLMs). The mapped queries preserve the SQL skeleton but adapt table/column names and constants according to the target schema and sample data.

This repository supports:
- Generating schema-aware query mappings using one-shot prompting
- Structural abstraction and transformation of SQL queries
- Semantic and execution-based filtering of generated queries

<p align="right">(<a href="#readme-top">back to top</a>)</p>

## 📁 Repository Structure

### Top Level Structure

```
├── data/                   # BIRD and SPIDER schemas, queries, and samples used in the pipeline  
├── query_generation/       # Scripts for mapping queries across schemas  
├── query_evaluation/       # Scripts for structural, semantic, and execution evaluations  
├── prompts/                # Prompt templates for mapping and evaluation  
├── mapping_settings.json   # Configuration for the query mapping stage  
├── evaluation_settings.json # Configuration for the evaluation stage  
├── run.py                  # Entry point to run the mapping pipeline  
├── requirements.txt        # Python dependencies  
└── README.md               # This file  
```

### Dataset Structure

Each dataset (`bird_dev`, `bird_training`, `spider_training`, `spider_dev`) includes:

```
<dataset_name>/
├── questions/                  # Source questions (if used as the source dataset)  
│   └── question_{db_id}.json  
├── target_samples/             # Target-side sample data (if used as the target dataset)  
│   └── sample_{db_id}.json  
└── schemas.json                # Schemas for all databases in the dataset  

```

### 📤 Output Structure

<details>
<summary>Expand to view</summary>

Outputs from SQL-Exchange are organized into the following main directories:

```
├── mappings/                                      # Generated mappings before evaluation  
│   ├── bird_dev/                 # development set used as target databases for mapping queries
│   │   ├── gemini-1.5-flash/         # model used to generate the mappings
│   │   └── gpt-4o-mini/              # model used to generate the mappings
│   └── spider_dev/  
│       └── gemini-1.5-flash/  
```

```
├── mappings_full_analysis/                        # Full mapping logs and stats  
│   └── gemini-1.5-flash/  
│       ├── 2025-05-19_23-33-25/                    # Timestamped run folder  
│       │   ├── bird_dev_california_schools/        # Source-target db mapping group  
│       │   ├── bird_dev_student_club/              # Another group  
│       │   ├── mapping_settings.json               # Settings used in this run  
│       │   └── stats.json                          # Stats summary for this mapping session  
│       ├── 2025-05-20_03-46-28/                    # Another run...  
│       └── 2025-05-20_03-47-32/  
```

```
├── evaluated_mappings/                            # Stores detailed mapping outputs with model responses  
│   ├── bird_dev/  
│   └── spider_dev/  
│       └── gemini-1.5-flash/                 # Model used to generate the mappings
│           └── car_1/                              # Example query group (varies by db_id)  
│           │    ├── llm_responses/                  # Raw LLM outputs for semantic evaluation  
│           │       ├── response_academic_llm.json  # LLM response for semantic evaluation of mappings between academic and car_1    
│           │       ├── response_device_llm.json    
│           │    ├── response_academic.json       # Evaluated json file for mapping between academic and car_1
│           │    └── response_device.json         # Evaluated json file for mapping between device and car_1
│           └── orchestra/
```

```
├── evaluated_mappings_summary/                    # Aggregated evaluation summaries  
│   ├── bird_dev/  
│   └── spider_dev/  
│       └── gemini-1.5-flash/                  # Model used to generate the mappings
│           ├── execution_summary/                  # Executability evaluations  
│           │     ├── full_summary/                   # Fine-grained summary per query and db_id  
│           │     ├── summary/                        # Coarse summaries  
│           │     ├── car_1.json                      # Summary for car_1 queries  
│           │     └── orchestra.json                  # Summary for orchestra queries  
│           ├── semantic_summary/                   # Semantic quality evaluations  
│           └── template_summary/                   # Structural similarity evaluations  
```

> 💡 **Note:**
>
> * `car_1` and `orchestra` are placeholders for different query clusters (based on `db_id`).
> * Timestamped folders under `mappings_full_analysis` correspond to individual mapping runs, which are useful for tracking experimental results.
> * Summaries are structured to support evaluation (semantic, structural, and execution metrics).

</details>


<p align="right">(<a href="#readme-top">back to top</a>)</p>

## 📋 Data Structure

### Questions Json

question_{db_id}.json
```json
[
  // Question 1
  {
    "dataset": "{dataset_name without directory}",
    "db_id": "{db_id}",
    "question": "{Natural language question}",
    "query": "{The query matching the question}"
  },
  // Question 2
  {...},
  ...
]
```

### Samples Json

sample_{db_id}.json
```json
{
  "{table_name}": [
    // Sample Row 1
    [
      // Raw values in the same order as columns
      "{col 1 val}",
      "{col 2 val}",
    ],
    // Sample Row 2 and so on
    [...]
  ],
  // more tables
  "{table_name}": [
    ...
  ],
  ...
}
```

### Schemas Json

schemas.json
```json
{
  // Schema 1
  "{db_id}": "{full schemas as a string}",  // The string should include characters such as next line character
  ...
}
```

<p align="right">(<a href="#readme-top">back to top</a>)</p>

---

## 🧪 Reproducing Results

### ✅ Prerequisites

* Python 3.12 installed
* Access to OpenAI and Gemini API keys

---

### ⚙️ Installation

1. **Install dependencies**

   ```bash
   pip install -r requirements.txt
   ```

2. **Set API keys**

   Set the environment variables before running the pipeline:

   ```bash
   export OPENAI_API_KEY=your_key
   export GEMINI_API_KEY=your_key
   ```

---

### 🚀 Running the Mapping and Evaluation Pipeline

#### 1. Run LLM-Based Query Mapping

* Edit the configuration file: [`mapping_settings.json`](#)
* Run the mapping process:

  ```bash
  python run.py
  ```

This generates mapped SQL queries for each target database, stored under:

```
mappings/{dataset_name}/{model_name}/target_db/response_{source_db}.json
```

---

#### 2. Evaluate Structural Alignment

* Edit evaluation configuration: [`evaluation_settings.json`](#)
* Run structural (template-based) evaluation:

  ```bash
  python query_evaluation/eval_template.py
  ```

Results are saved in the `evaluated_mappings/` directory under the respective dataset and model folders. Each subfolder corresponds to a target database, containing evaluated mappings with structural labels.

---

#### 3. Evaluate Executability

To assess whether mapped queries can be executed on real databases:

1. Prepare the development set SQLite databases for BIRD and SPIDER:

```
raw_datasets/
├── bird_dev/
│   └── dev_databases/
│       ├── california_schools/
│       │   └── california_schools.sqlite
│       ├── card_games/
│       └── ...
└── spider_dev/
    └── dev_databases/
        ├── car_1/
        │   └── car_1.sqlite
        ├── battle_death/
        └── ...
```

2. Run execution evaluation:

   ```bash
   python query_evaluation/eval_execution.py
   ```

This adds execution correctness labels to previously evaluated files in `evaluated_mappings/`.

---

#### 4. Evaluate Semantic Quality

To assess the quality of the natural language (NL) and SQL alignment:

* Run the semantic evaluator (uses `gemini-1.5-flash` or `gemini-2.0-flash`):

  ```bash
  python query_evaluation/eval_semantic.py
  ```

This step evaluates:

* Whether the NL question is meaningful
* Whether the SQL query correctly answers the question

Results are added to `evaluated_mappings/`, along with LLM-generated reasoning per query.

---

### ⚙️ Mapping Settings

<details>
<summary>Click to expand <code>mapping_settings.json</code></summary>

```json
{
  "model": {
    // The origin/provider of the model (e.g., "google" or "openai")
    "model_origin": "google",

    // Specific model version used for query generation
    "model_name": "gemini-1.5-flash",

    // Optional token limit for input prompts; null = no enforced limit
    "input_token_limit": null,

    // Whether to use a system-level instruction at the start of prompts
    "use_system_instruction": true,

    // Sampling temperature (0 = deterministic)
    "temperature": 0,

    // Top-p sampling cutoff
    "top_p": 1,

    "google": {
      // Top-k setting specific to Gemini models
      "top_k": 0
    }
  },

  "generation": {
    // Whether to store a copy of the settings inside the output files
    "copy_settings_to_output": true,

    // Maximum number of NL questions allowed per prompt
    "max_question_length_per_prompt": 10,

    // Number of retries per failed prompt generation attempt
    "max_retry_per_prompt": 3,

    // Maximum total failures before aborting the full run
    "max_fail_limit": 100,

    "validation": {
      // Whether to check for presence of required fields in LLM output
      "fields_checking": true,

      // Whether to verify that db_id in response matches expected target
      "db_id_matching": true
    },

    // Method use to map queries: sql-exchange or zeroshot
    "method": "sql-exchange",

    // Directory where prompt templates are stored
    "prompt_directory": "prompts",

    // Base prompt template file for few-shot prompting
    "base_prompt_file": "mapping_base.txt",

    // System message file to be prepended to prompts for SQL-Exchange method (if enabled)
    "system_instruction_file": "mapping_system.txt",

    // System message file to be prepended to prompts for zeroshot method (if enabled)
    "system_instruction_file_zeroshot": "zeroshot_system.txt",

    // Output folder for full mapping logs (includes intermediate metadata and errors)
    "output_directory": "mappings_full_analysis",

    // Output folder storing only the final JSON results for SQL-Exchange
    "json_only_output_directory": "mappings",

    // Output folder storing only the final JSON results for zeroshot method
    "json_only_output_directory_zeroshot": "mappings_zeroshot",

    // Fields required to be present in the LLM's structured output
    "fields_to_check": [
      "source_dataset",
      "source_db_id",
      "source_query",
      "source_question",
      "tables_columns_replacement",
      "thought",
      "target_db_id",
      "target_query",
      "target_question"
    ],
    "fields_to_check_zeroshot": [
            "source_dataset",
            "source_db_id",
            "source_query",
            "source_question",
            "target_db_id",
            "target_query",
            "target_question"
        ]
  },

  "data": [
    {
      // Dataset containing the source databases and questions
      "source_dataset": "data/bird_training",

      // Subset of source database IDs to sample from
      "source_db_ids": [
        "address",
        "books"
      ],

      // Seed for shuffling questions inside each source database
      "source_questions_shuffle_seed": 12,

      // Max number of questions per source db (after shuffling)
      "source_questions_limit": 20,

      // Dataset that contains the target schema
      "target_dataset": "data/bird_dev",

      // Target database ID for mapping
      "target_db_id": "california_schools"
    },
    {
      "source_dataset": "data/bird_training",
      "source_db_ids": [
        "beer_factory"
      ],
      "source_questions_shuffle_seed": 12,
      "source_questions_limit": 20,
      "target_dataset": "data/bird_dev",
      "target_db_id": "student_club"
    }
  ]
}
```

</details>


### ⚙️ Evaluation Settings

<details>
<summary>Click to expand <code>evaluation_settings.json</code></summary>

```json
{
  "model": {
    // Base model used for evaluation (e.g., semantic evaluation)
    "model_name": "gemini",
    "model_version": "gemini-2.0-flash",
    "input_token_limit": null,
    "use_system_instruction": true,
    "temperature": 0,
    "top_p": 1,
    "google": {
      // Top-k setting for Gemini; set to 0 for deterministic output
      "top_k": 0
    }
  },

  "data": {
    // Optionally restrict to a subset of source or target databases
    // Leave empty to evaluate all available mappings
    "source_databases": [],
    "target_databases": ["california_schools"]
  },

  "evaluation": {

    // The method which you want to evaluate
    "method": "sql-exchange",

    // Dataset where target databases are taken from (e.g., bird_dev, spider_dev)
    "dataset_name": "bird_dev",

    // Folder name of the model used to generate mappings (under mappings/)
    "model_dir": "gemini-1.5-flash",

    // Input folder for mapped SQLs
    "generated_queries_directory": "mappings",

    // Output folder for storing evaluated results for SQL-Exchange method
    "result_directory": "evaluated_mappings",

    // Output folder for aggregated summary metrics for SQL-Exchange method
    "summary_directory": "evaluated_mappings_summary",

    // Output folder for storing evaluated results for zeroshot method
    "result_directory_zeroshot": "evaluated_mappings_zeroshot",

    // Output folder for aggregated summary metrics for zeroshot method
    "summary_directory_zeroshot": "evaluated_mappings_summary_zeroshot",

    // Folder name where LLM semantic evaluation responses are saved (subfolder of each db_id)
    "llm_response_directory": "llm_responses",

    // Folder name for storing rule-based or non-LLM responses (e.g., execution results)
    "response_directory": "responses",

    // Retry logic for failed prompts during LLM-based evaluation
    "max_retry_per_prompt": 5,
    "sleep_time": 4,

    // Path to SQLite database files (used in execution evaluation)
    "raw_datasets_directory": "raw_datasets",

    // Directory for prompt templates used in semantic evaluation
    "prompt_directory": "prompts",

    // Few-shot examples used in semantic evaluation (LLM input)
    "examples_file": "evaluation_examples.txt",

    // Base prompt file used for semantic evaluation
    "prompt_file": "evaluation_base.txt"
  }
}
```

</details>

<p align="right">(<a href="#readme-top">back to top</a>)</p>

## 📊 Dataset

- BIRD benchmark [Website](https://bird-bench.github.io/)
- SPIDER benchmark [Website](https://yale-lily.github.io/spider)

We use training databases as sources and development databases as targets, generating over 100K SQL-NL pairs.

<p align="right">(<a href="#readme-top">back to top</a>)</p>

## 📢 Notice

**Code and data will be made publicly available upon acceptance.**