# Middleware Examples

This folder contains interactive Jupyter notebooks demonstrating various middleware patterns with the Agent Framework. Middleware allows you to intercept and modify behavior at different execution stages, including agent runs, function calls, and chat interactions.

## Interactive Notebooks

| Notebook | Description |
|----------|-------------|
| [`1-agent_and_run_level_middleware.ipynb`](1-agent_and_run_level_middleware.ipynb) | Interactive tutorial on agent-level vs run-level middleware with examples |
| [`2-function_based_middleware.ipynb`](2-function_based_middleware.ipynb) | Function-based middleware patterns with hands-on examples |
| [`3-class_based_middleware.ipynb`](3-class_based_middleware.ipynb) | Class-based middleware using inheritance patterns |
| [`4-decorator_middleware.ipynb`](4-decorator_middleware.ipynb) | Decorator-based middleware with @agent_middleware and @function_middleware |
| [`5-chat_middleware.ipynb`](5-chat_middleware.ipynb) | Chat middleware for message interception and modification |
| [`6-exception_handling_with_middleware.ipynb`](6-exception_handling_with_middleware.ipynb) | Exception handling patterns with middleware |
| [`7-middleware_termination.ipynb`](7-middleware_termination.ipynb) | Termination patterns for early exit scenarios |
| [`8-override_result_with_middleware.ipynb`](8-override_result_with_middleware.ipynb) | Result override for streaming and non-streaming responses |
| [`9-shared_state_middleware.ipynb`](9-shared_state_middleware.ipynb) | Shared state patterns with middleware container classes |

## Getting Started

### Prerequisites

1. **Azure AI Foundry Project**: Set up an Azure AI Foundry project
2. **Environment Variables**: Configure `.env` file in the parent directory with:
   - `AZURE_AI_PROJECT_ENDPOINT`: Your Azure AI Foundry project endpoint
   - `AZURE_AI_MODEL_DEPLOYMENT_NAME`: Your model deployment name (e.g., gpt-4o)
3. **Python Environment**: Python 3.12+ with the Agent Framework package installed
4. **Authentication**: Run `az login` for Azure CLI authentication

### Running the Notebooks

1. Open any notebook (1-9) in VS Code or Jupyter
2. Select the Python kernel (`.venv` recommended)
3. Run cells sequentially to see middleware patterns in action
4. Experiment by modifying middleware logic and re-running

## Key Concepts

### Middleware Types

- **Agent Middleware**: Intercepts agent run execution, allowing you to modify requests and responses
- **Function Middleware**: Intercepts function calls within agents, enabling logging, validation, and result modification
- **Chat Middleware**: Intercepts chat requests sent to AI models, allowing input/output transformation

### Implementation Approaches

- **Function-based**: Simple async functions for lightweight, stateless operations
- **Class-based**: Inherit from base middleware classes for complex, stateful operations
- **Decorator-based**: Use decorators for explicit middleware marking

### Common Use Cases

- **Security**: Validate requests, block sensitive information, implement access controls
- **Logging**: Track execution timing, log parameters and results, monitor performance
- **Error Handling**: Catch exceptions, provide graceful fallbacks, implement retry logic
- **Result Transformation**: Filter, format, or enhance function outputs
- **State Management**: Share data between middleware functions, maintain execution context

### Execution Control

- **Termination**: Use `context.terminate` to stop execution early
- **Result Override**: Modify or replace function/agent results
- **Streaming Support**: Handle both regular and streaming responses