# Microsoft Agent Framework

## 🤖 Introduction

The **Microsoft Agent Framework** is an open-source development kit for building **AI agents** and **multi-agent workflows** for .NET and Python. It brings together and extends ideas from the Semantic Kernel and AutoGen projects, combining their strengths while adding new capabilities. Built by the same teams, it is the unified foundation for building AI agents going forward.

The Agent Framework offers two primary categories of capabilities:

### � AI Agents
Individual agents that use LLMs to process user inputs, call tools and MCP servers to perform actions, and generate responses. AI agents are suitable for applications that require:
- **Autonomous Decision-Making**: Dynamic problem-solving without predefined sequences
- **Tool Integration**: Seamless interaction with external APIs, databases, and services
- **Conversation Management**: Multi-turn conversations with persistent context
- **Model Flexibility**: Support for Azure OpenAI, OpenAI, and Azure AI providers

### 🔄 Workflows  
Graph-based workflows that connect multiple agents and functions to perform complex, multi-step tasks. Workflows provide:
- **Structured Orchestration**: Explicit control over multi-agent execution paths
- **Type Safety**: Strong typing with comprehensive validation
- **Checkpointing**: State persistence for long-running processes
- **Human-in-the-Loop**: Built-in patterns for human interaction and approval

### 🎯 Why Agent Framework?

The Agent Framework is the **direct successor** to Semantic Kernel and AutoGen, created by the same teams. It combines:
- **AutoGen's** simple abstractions for single- and multi-agent patterns
- **Semantic Kernel's** enterprise-grade features (thread management, type safety, telemetry)
- **New capabilities** including robust workflows and advanced state management

> **Note**: Microsoft Agent Framework is currently in **public preview**. This represents the next generation of both Semantic Kernel and AutoGen.

### 📚 Learn More

- 📖 **Official Documentation**: [Agent Framework Overview](https://learn.microsoft.com/en-us/agent-framework/overview/agent-framework-overview)
- 🔗 **GitHub Repository**: [Microsoft Agent Framework](https://github.com/microsoft/agent-framework) - Additional examples and source code
- 🚀 **Get Started**: Follow the notebooks below for hands-on learning

---

This folder contains examples demonstrating different ways to create and use agents with the Azure AI chat client from the `agent_framework.azure` package.

## 📚 Available Notebooks

### Basic Examples
- **`azure_ai_basic.ipynb`** - Basic Azure AI agent usage with automatic lifecycle management
- **`azure_ai_with_explicit_settings.ipynb`** - Creating agents with explicit configuration settings

### Agent Management
- **`azure_ai_with_existing_agent.ipynb`** - Working with pre-existing agents using agent IDs
- **`azure_ai_with_existing_thread.ipynb`** - Managing conversation threads for continuity

### Tool Integration
- **`azure_ai_with_function_tools.ipynb`** - Comprehensive function tool integration patterns
- **`azure_ai_with_code_interpreter.ipynb`** - Python code execution and mathematical problem solving
- **`azure_ai_with_file_search.ipynb`** - Document-based question answering with file uploads

### Advanced Features
- **`azure_ai_with_bing_grounding.ipynb`** - Web search integration using Bing Grounding

## 🚀 Getting Started

### Prerequisites

1. **Install Dependencies**: Make sure you have the agent-framework packages installed. Install in your virtual environment:

   **Alternative Installation Options:**

   **Development mode** - Install the entire framework with all sub-packages (simplest approach):
   ```bash
   pip install agent-framework --pre
   ```
   This installs the core and every integration package, making sure that all features are available without additional steps. The `--pre` flag is required while Agent Framework is in preview.

   **Selective install** - Install only specific integrations to keep dependencies lighter:
   ```bash
   # Core only (includes Azure OpenAI and OpenAI support by default)
   pip install agent-framework-core --pre

   # Core + Azure AI integration
   pip install agent-framework-azure-ai --pre

   # Core + Microsoft Copilot Studio integration
   pip install agent-framework-copilotstudio --pre

   # Core + both Microsoft Copilot Studio and Azure AI integration
   pip install agent-framework-microsoft agent-framework-azure-ai --pre
   ```

   **Supported Platforms:** Python 3.10+, Windows/macOS/Linux

2. **Azure Authentication**: Authenticate with Azure CLI:
   ```bash
   az login
   ```

3. **Environment Configuration**: Set up your environment variables by copying the sample file:

   Copy the .env_sample file to create a .env file:
   ```bash
   cp .env_sample .env
   ```

   Edit the .env file and add your values:
   ```
   AZURE_AI_PROJECT_ENDPOINT="your-project-endpoint"
   AZURE_AI_MODEL_DEPLOYMENT_NAME="your-model-deployment-name"
   TENANT_ID="your-tenant-id"
   ```

   For samples using Bing Grounding search (like `azure_ai_with_bing_grounding.ipynb`), you'll also need:
   ```
   BING_CONNECTION_ID="/subscriptions/{subscription-id}/resourceGroups/{resource-group}/providers/Microsoft.CognitiveServices/accounts/{ai-service-name}/projects/{project-name}/connections/{connection-name}"
   ```

   To get your Bing connection ID:
   - Go to Azure AI Foundry portal
   - Navigate to your project's "Connected resources" section
   - Add a new connection for "Grounding with Bing Search"
   - Copy the connection ID

### Running the Notebooks

1. **Start Jupyter**: Launch Jupyter Notebook or JupyterLab from this folder:
   ```bash
   jupyter notebook
   ```

2. **Open Notebooks**: Open any of the `.ipynb` files in this directory

3. **Execute**: Run the cells in order, following the explanations and examples

## 📖 Learning Path

### Beginner
1. Start with `azure_ai_basic.ipynb` to understand fundamental concepts
2. Try `azure_ai_with_explicit_settings.ipynb` for configuration patterns
3. Explore `azure_ai_with_function_tools.ipynb` for tool integration

### Intermediate
1. Learn agent persistence with `azure_ai_with_existing_agent.ipynb`
2. Master conversation management with `azure_ai_with_existing_thread.ipynb`
3. Implement document search with `azure_ai_with_file_search.ipynb`

### Advanced
1. Integrate web search with `azure_ai_with_bing_grounding.ipynb`
2. Execute code dynamically with `azure_ai_with_code_interpreter.ipynb`

## 🔧 Key Features Demonstrated

### Core Concepts
- ✅ Agent creation and lifecycle management
- ✅ Authentication patterns with Azure CLI
- ✅ Environment configuration and best practices
- ✅ Error handling and resource cleanup

### Tool Integration
- ✅ Function tools for custom capabilities
- ✅ Code interpreter for Python execution
- ✅ File search for document-based Q&A
- ✅ Web search with Bing integration
- ✅ External API integration via OpenAPI

### Advanced Patterns
- ✅ Multi-turn conversations
- ✅ Thread management and persistence
- ✅ Agent reuse patterns
- ✅ Tool combination strategies
- ✅ User approval workflows

## 💡 Tips for Success

1. **Environment Setup**: Ensure all environment variables are properly configured
2. **Authentication**: Keep your Azure CLI session active
3. **Resource Management**: Always clean up resources to avoid unnecessary costs
4. **Error Handling**: Pay attention to error handling patterns in the examples
5. **Best Practices**: Follow the documented best practices for production usage

## 🛠️ Troubleshooting

### Common Issues

1. **Authentication Errors**: Run `az login` and verify your credentials
2. **Missing Environment Variables**: Check that all required variables are set
3. **Package Import Errors**: Ensure agent-framework packages are installed
4. **Resource Not Found**: Verify your Azure AI project configuration

