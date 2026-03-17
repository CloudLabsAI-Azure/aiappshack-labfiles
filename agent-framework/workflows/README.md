# Workflows Getting Started Samples

## Installation

Microsoft Agent Framework Workflows support ships with the core `agent-framework` or `agent-framework-core` package, so no extra installation step is required.

To install with visualization support:

```bash
pip install agent-framework[viz] --pre
```

To export visualization images you also need to [install GraphViz](https://graphviz.org/download/).

## Samples Overview

## Foundational Concepts - Start Here

Begin with the `_start-here` folder in order. These three samples introduce the core ideas of executors, edges, agents in workflows, and streaming.

| Sample | Notebook | Concepts |
|--------|------|----------|
| Executors and Edges | [_start-here/notebooks/step1_executors_and_edges.ipynb](./_start-here/notebooks/step1_executors_and_edges.ipynb) | Minimal workflow with basic executors and edges |
| Agents in a Workflow | [_start-here/notebooks/step2_agents_in_a_workflow.ipynb](./_start-here/notebooks/step2_agents_in_a_workflow.ipynb) | Introduces adding Agents as nodes; calling agents inside a workflow |
| Streaming (Basics) | [_start-here/notebooks/step3_streaming.ipynb](./_start-here/notebooks/step3_streaming.ipynb) | Extends workflows with event streaming |

Once comfortable with these, explore the rest of the samples below.

---

## Samples Overview (by directory)

### agents

| Sample | Notebook | Concepts |
|---|---|---|
| Azure Chat Agents (Streaming) | [agents/notebooks/azure_chat_agents_streaming.ipynb](./agents/notebooks/azure_chat_agents_streaming.ipynb) | Add Azure Chat agents as edges and handle streaming events |
| Azure AI Chat Agents (Streaming) | [agents/notebooks/azure_ai_agents_streaming.ipynb](./agents/notebooks/azure_ai_agents_streaming.ipynb) | Add Azure AI agents as edges and handle streaming events |
| Azure Chat Agents (Function Bridge) | [agents/notebooks/azure_chat_agents_function_bridge.ipynb](./agents/notebooks/azure_chat_agents_function_bridge.ipynb) | Chain two agents with a function executor that injects external context |
| Azure Chat Agents (Tools + HITL) | [agents/notebooks/azure_chat_agents_tool_calls_with_feedback.ipynb](./agents/notebooks/azure_chat_agents_tool_calls_with_feedback.ipynb) | Tool-enabled writer/editor pipeline with human feedback gating via RequestInfoExecutor |
| Custom Agent Executors | [agents/notebooks/custom_agent_executors.ipynb](./agents/notebooks/custom_agent_executors.ipynb) | Create executors to handle agent run methods |
| Workflow as Agent (Reflection Pattern) | [agents/notebooks/workflow_as_agent_reflection_pattern.ipynb](./agents/notebooks/workflow_as_agent_reflection_pattern.ipynb) | Wrap a workflow so it can behave like an agent (reflection pattern) |
| Workflow as Agent + HITL | [agents/notebooks/workflow_as_agent_human_in_the_loop.ipynb](./agents/notebooks/workflow_as_agent_human_in_the_loop.ipynb) | Extend workflow-as-agent with human-in-the-loop capability |

### checkpoint

| Sample | Notebook | Concepts |
|---|---|---|
| Checkpoint & Resume | [checkpoint/notebooks/checkpoint_with_resume.ipynb](./checkpoint/notebooks/checkpoint_with_resume.ipynb) | Create checkpoints, inspect them, and resume execution |
| Checkpoint & HITL Resume | [checkpoint/notebooks/checkpoint_with_human_in_the_loop.ipynb](./checkpoint/notebooks/checkpoint_with_human_in_the_loop.ipynb) | Combine checkpointing with human approvals and resume pending HITL requests |
| Checkpointed Sub-Workflow | [checkpoint/notebooks/sub_workflow_checkpoint.ipynb](./checkpoint/notebooks/sub_workflow_checkpoint.ipynb) | Save and resume a sub-workflow that pauses for human approval |

### composition

| Sample | Notebook | Concepts |
|---|---|---|
| Sub-Workflow (Basics) | [composition/notebooks/sub_workflow_basics.ipynb](./composition/notebooks/sub_workflow_basics.ipynb) | Wrap a workflow as an executor and orchestrate sub-workflows |
| Sub-Workflow: Request Interception | [composition/notebooks/sub_workflow_request_interception.ipynb](./composition/notebooks/sub_workflow_request_interception.ipynb) | Intercept and forward sub-workflow requests using @handler for RequestInfoMessage subclasses |
| Sub-Workflow: Parallel Requests | [composition/notebooks/sub_workflow_parallel_requests.ipynb](./composition/notebooks/sub_workflow_parallel_requests.ipynb) | Multiple specialized interceptors handling different request types from same sub-workflow |

### control-flow

| Sample | Notebook | Concepts |
|---|---|---|
| Sequential Executors | [control-flow/notebooks/sequential_executors.ipynb](./control-flow/notebooks/sequential_executors.ipynb) | Sequential workflow with explicit executor setup |
| Sequential (Streaming) | [control-flow/notebooks/sequential_streaming.ipynb](./control-flow/notebooks/sequential_streaming.ipynb) | Stream events from a simple sequential run |
| Edge Condition | [control-flow/notebooks/edge_condition.ipynb](./control-flow/notebooks/edge_condition.ipynb) | Conditional routing based on agent classification |
| Switch-Case Edge Group | [control-flow/notebooks/switch_case_edge_group.ipynb](./control-flow/notebooks/switch_case_edge_group.ipynb) | Switch-case branching using classifier outputs |
| Multi-Selection Edge Group | [control-flow/notebooks/multi_selection_edge_group.ipynb](./control-flow/notebooks/multi_selection_edge_group.ipynb) | Select one or many targets dynamically (subset fan-out) |
| Simple Loop | [control-flow/notebooks/simple_loop.ipynb](./control-flow/notebooks/simple_loop.ipynb) | Feedback loop where an agent judges ABOVE/BELOW/MATCHED |

### human-in-the-loop

| Sample | Notebook | Concepts |
|---|---|---|
| Human-In-The-Loop (Guessing Game) | [human-in-the-loop/notebooks/guessing_game_with_human_input.ipynb](./human-in-the-loop/notebooks/guessing_game_with_human_input.ipynb) | Interactive request/response prompts with a human |
| Azure Agents Tool Feedback Loop | [agents/notebooks/azure_chat_agents_tool_calls_with_feedback.ipynb](./agents/notebooks/azure_chat_agents_tool_calls_with_feedback.ipynb) | Two-agent workflow that streams tool calls and pauses for human guidance between passes |

### observability

| Sample | Notebook | Concepts |
|---|---|---|
| Tracing (Basics) | [observability/notebooks/tracing_basics.ipynb](./observability/notebooks/tracing_basics.ipynb) | Use basic tracing for workflow telemetry. Refer to this [directory](../observability/) to learn more about observability concepts. |

### orchestration

| Sample | Notebook | Concepts |
|---|---|---|
| Concurrent Orchestration (Default Aggregator) | [orchestration/notebooks/concurrent_agents.ipynb](./orchestration/notebooks/concurrent_agents.ipynb) | Fan-out to multiple agents; fan-in with default aggregator returning combined ChatMessages |
| Concurrent Orchestration (Custom Aggregator) | [orchestration/notebooks/concurrent_custom_aggregator.ipynb](./orchestration/notebooks/concurrent_custom_aggregator.ipynb) | Override aggregator via callback; summarize results with an LLM |
| Concurrent Orchestration (Custom Agent Executors) | [orchestration/notebooks/concurrent_custom_agent_executors.ipynb](./orchestration/notebooks/concurrent_custom_agent_executors.ipynb) | Child executors own ChatAgents; concurrent fan-out/fan-in via ConcurrentBuilder |
| Magentic Workflow (Multi-Agent) | [orchestration/notebooks/magentic.ipynb](./orchestration/notebooks/magentic.ipynb) | Orchestrate multiple agents with Magentic manager and streaming |
| Magentic + Human Plan Review | [orchestration/notebooks/magentic_human_plan_update.ipynb](./orchestration/notebooks/magentic_human_plan_update.ipynb) | Human reviews/updates the plan before execution |
| Magentic + Checkpoint Resume | [orchestration/notebooks/magentic_checkpoint.ipynb](./orchestration/notebooks/magentic_checkpoint.ipynb) | Resume Magentic orchestration from saved checkpoints |
| Sequential Orchestration (Agents) | [orchestration/notebooks/sequential_agents.ipynb](./orchestration/notebooks/sequential_agents.ipynb) | Chain agents sequentially with shared conversation context |
| Sequential Orchestration (Custom Executor) | [orchestration/notebooks/sequential_custom_executors.ipynb](./orchestration/notebooks/sequential_custom_executors.ipynb) | Mix agents with a summarizer that appends a compact summary |

**Magentic checkpointing tip**: Treat `MagenticBuilder.participants` keys as stable identifiers. When resuming from a checkpoint, the rebuilt workflow must reuse the same participant names; otherwise the checkpoint cannot be applied and the run will fail fast.

### parallelism

| Sample | Notebook | Concepts |
|---|---|---|
| Concurrent (Fan-out/Fan-in) | [parallelism/notebooks/fan_out_fan_in_edges.ipynb](./parallelism/notebooks/fan_out_fan_in_edges.ipynb) | Dispatch to multiple executors and aggregate results |
| Aggregate Results of Different Types | [parallelism/notebooks/aggregate_results_of_different_types.ipynb](./parallelism/notebooks/aggregate_results_of_different_types.ipynb) | Handle results of different types from multiple concurrent executors |
| Map-Reduce with Visualization | [parallelism/notebooks/map_reduce_and_visualization.ipynb](./parallelism/notebooks/map_reduce_and_visualization.ipynb) | Fan-out/fan-in pattern with diagram export |

### state-management

| Sample | Notebook | Concepts |
|---|---|---|
| Shared States | [state-management/notebooks/shared_states_with_agents.ipynb](./state-management/notebooks/shared_states_with_agents.ipynb) | Store in shared state once and later reuse across agents |

### visualization

| Sample | Notebook | Concepts |
|---|---|---|
| Concurrent with Visualization | [visualization/notebooks/concurrent_with_visualization.ipynb](./visualization/notebooks/concurrent_with_visualization.ipynb) | Fan-out/fan-in workflow with diagram export |

### resources

- Sample text inputs used by certain workflows:
  - [resources/long_text.txt](./resources/long_text.txt)
  - [resources/email.txt](./resources/email.txt)
  - [resources/spam.txt](./resources/spam.txt)
  - [resources/ambiguous_email.txt](./resources/ambiguous_email.txt)

Notes

- Agent-based samples use provider SDKs (Azure/OpenAI, etc.). Ensure credentials are configured, or adapt agents accordingly.

Sequential orchestration uses a few small adapter nodes for plumbing:

- "input-conversation" normalizes input to `list[ChatMessage]`
- "to-conversation:<participant>" converts agent responses into the shared conversation
- "complete" publishes the final `WorkflowOutputEvent`
These may appear in event streams (ExecutorInvoke/Completed). They’re analogous to
concurrent’s dispatcher and aggregator and can be ignored if you only care about agent activity.

### Environment Variables

- **AzureOpenAIChatClient**: Set Azure OpenAI environment variables as documented [here](https://github.com/microsoft/agent-framework/blob/main/python/samples/getting_started/chat_client/README.md#environment-variables).
  These variables are required for samples that construct `AzureOpenAIChatClient`

- **OpenAI** (used in orchestration samples):
  - [OpenAIChatClient env vars](https://github.com/microsoft/agent-framework/blob/main/python/samples/getting_started/agents/openai_chat_client/README.md)
  - [OpenAIResponsesClient env vars](https://github.com/microsoft/agent-framework/blob/main/python/samples/getting_started/agents/openai_responses_client/README.md)
