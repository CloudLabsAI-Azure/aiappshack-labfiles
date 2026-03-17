# Context Providers - Agent Memory & Context Management

This folder demonstrates how to build AI agents with **memory capabilities** using **Context Providers** in the Microsoft Agent Framework. Context providers enable agents to extract, store, and inject relevant context before and after each agent invocation, creating personalized and stateful conversational experiences.

## 🧠 Overview

Context providers are a powerful pattern for implementing agent memory. They allow you to:

- **Extract Information**: Automatically capture facts, preferences, or insights from conversations
- **Store Memories**: Persist user information across sessions
- **Inject Context**: Provide relevant historical information before each agent invocation
- **Compose Providers**: Combine multiple context providers using `AggregateContextProvider`

### Key Concepts

A `ContextProvider` observes the agent lifecycle through three key methods:

1. **`thread_created()`** - Triggered when a new conversation thread is initialized
2. **`invoking()`** - Runs before each agent invocation to inject additional context (instructions, messages, or tools)
3. **`invoked()`** - Runs after the agent responds to extract and store new information

## 📓 Available Notebooks

### Interactive Examples

| Notebook | Description | Key Features |
|----------|-------------|--------------|
| **`1-azure_ai_memory_context_providers.ipynb`** | Complete agent memory implementation with Azure AI Foundry | • User fact extraction<br>• In-memory fact storage<br>• Tone/style providers<br>• Multi-provider composition<br>• Thread-based conversations |

## 🚀 Quick Start

### Prerequisites

1. **Install Dependencies**:
   ```bash
   pip install agent-framework agent-framework-azure-ai python-dotenv azure-identity
   ```

2. **Azure Authentication**:
   ```bash
   az login
   ```

3. **Environment Setup**: Configure your `.env` file in `../agents/azure_ai_agents/`:
   ```
   AZURE_AI_PROJECT_ENDPOINT="your-project-endpoint"
   AZURE_AI_MODEL_DEPLOYMENT_NAME="your-model-deployment-name"
   ```

### Running the Notebook

1. **Navigate to the folder**:
   ```bash
   cd context_providers
   ```

2. **Launch Jupyter**:
   ```bash
   jupyter notebook
   ```

3. **Open and run**: `1-azure_ai_memory_context_providers.ipynb`

## 🎯 What You'll Learn

### 1. Custom Context Providers

**UserFactContextProvider** - Extracts and stores user facts:
- Pattern-based fact extraction (name, location, preferences)
- In-memory storage with `FactStore`
- Automatic context injection before agent runs
- Configurable fact limits

**ToneContextProvider** - Maintains consistent response style:
- Lightweight instruction injection
- Composable with other providers
- Brand voice alignment

### 2. Provider Composition

**AggregateContextProvider** - Combines multiple providers:
```python
providers = AggregateContextProvider([
    ToneContextProvider(instructions="Use a professional tone"),
    UserFactContextProvider(user_id="user123", store=store),
])
```

### 3. Thread Management

**Critical Pattern**: Use a single thread for conversation continuity:
```python
# Create one thread for the entire conversation
thread = agent.get_new_thread()

# Reuse the thread across turns
response = await agent.run(prompt, thread=thread)
```

This ensures:
- Chat history is maintained
- Context providers can track state across turns
- Natural conversation flow without repetition

### 4. Memory Patterns

**In-Memory Storage** (Development):
- Simple `FactStore` implementation
- Perfect for notebooks and prototyping
- No external dependencies

**Production Storage** (Next Steps):
- Azure Cosmos DB for persistent storage
- Azure Cache for Redis for high-performance caching
- Azure Table Storage for structured data
- Custom `ChatMessageStore` implementations

## 🔍 Deep Dive: How Context Providers Work

### Lifecycle Hooks

```python
class CustomContextProvider(ContextProvider):
    async def thread_created(self, thread_id: str | None) -> None:
        """Initialize when a new thread is created"""
        pass
    
    async def invoking(
        self, 
        messages: ChatMessage | MutableSequence[ChatMessage], 
        **kwargs
    ) -> Context:
        """Inject context before agent invocation"""
        return Context(
            instructions="Additional context here",
            messages=[],  # Optional additional messages
            tools=[]      # Optional additional tools
        )
    
    async def invoked(
        self,
        request_messages: ChatMessage | Sequence[ChatMessage],
        response_messages: ChatMessage | Sequence[ChatMessage] | None = None,
        invoke_exception: Exception | None = None,
        **kwargs
    ) -> None:
        """Extract and store information after agent responds"""
        pass
```

### Example: Fact Extraction

The `UserFactContextProvider` demonstrates:
- **Pattern Matching**: Regular expressions to identify facts in user messages
- **Deduplication**: Ensures facts aren't stored multiple times
- **Caching**: Maintains cached facts for fast access
- **Selective Injection**: Only injects recent facts (configurable limit)

### Example: Multi-Turn Conversation

```python
async with (
    AzureCliCredential() as credential,
    ChatAgent(
        chat_client=AzureAIAgentClient(async_credential=credential),
        instructions="You are a helpful assistant.",
        context_providers=providers,
    ) as agent
):
    thread = agent.get_new_thread()
    
    # Turn 1: User introduces themselves
    await agent.run("My name is Alex", thread=thread)
    
    # Turn 2: Agent remembers the name
    await agent.run("What's my name?", thread=thread)
    # Response: "Your name is Alex"
```

## 🛠️ Implementation Patterns

### Pattern 1: Simple Memory Provider

```python
class SimpleMemory(ContextProvider):
    def __init__(self):
        self.facts = []
    
    async def invoking(self, messages, **kwargs) -> Context:
        if self.facts:
            context = "\n".join(self.facts)
            return Context(instructions=f"Known facts:\n{context}")
        return Context()
    
    async def invoked(self, request_messages, response_messages, **kwargs) -> None:
        # Extract and store facts
        pass
```

### Pattern 2: Persistent Memory Provider

```python
class DatabaseMemory(ContextProvider):
    def __init__(self, connection_string: str, user_id: str):
        self.connection = create_connection(connection_string)
        self.user_id = user_id
    
    async def invoking(self, messages, **kwargs) -> Context:
        # Load facts from database
        facts = await self.connection.load_facts(self.user_id)
        return Context(instructions=self._format_facts(facts))
    
    async def invoked(self, request_messages, response_messages, **kwargs) -> None:
        # Extract and save to database
        facts = self._extract_facts(request_messages)
        await self.connection.save_facts(self.user_id, facts)
```

### Pattern 3: Retrieval-Augmented Memory

```python
class RAGMemory(ContextProvider):
    def __init__(self, vector_store):
        self.vector_store = vector_store
    
    async def invoking(self, messages, **kwargs) -> Context:
        # Retrieve relevant memories from vector store
        query = messages[-1].text if messages else ""
        relevant = await self.vector_store.similarity_search(query)
        return Context(instructions=f"Relevant context:\n{relevant}")
```

## 📚 Related Documentation

- 📖 **[Agent Memory Documentation](https://learn.microsoft.com/en-us/agent-framework/user-guide/agents/agent-memory?pivots=programming-language-python)** - Official Microsoft documentation
- 📖 **[Adding Memory to Agents Tutorial](https://learn.microsoft.com/en-us/agent-framework/tutorials/agents/memory?pivots=programming-language-python)** - Step-by-step tutorial
- 🔗 **[Agent Framework GitHub](https://github.com/microsoft/agent-framework)** - Source code and additional examples

## 🎓 Learning Path

### Beginner
1. Start with `1-azure_ai_memory_context_providers.ipynb`
2. Understand the `FactStore` pattern
3. Run the demo conversation to see memory in action

### Intermediate
1. Modify the fact extraction patterns
2. Add new context providers (e.g., preference provider, history summarizer)
3. Experiment with different storage backends

### Advanced
1. Implement persistent storage with Azure Cosmos DB
2. Build RAG-based memory with vector search
3. Combine with middleware for security and validation
4. Create custom serialization for thread persistence

## 💡 Best Practices

### ✅ Do's
- **Use threads consistently**: Always pass the same thread object across conversation turns
- **Limit context size**: Control how many facts/memories are injected to avoid token limits
- **Handle errors gracefully**: Check for `invoke_exception` in the `invoked()` method
- **Cache frequently used data**: Store recent facts in memory for fast access
- **Validate extracted data**: Sanitize and validate facts before storage

### ❌ Don'ts
- **Don't create new threads per turn**: This breaks conversation continuity
- **Don't inject unlimited context**: Respect model context window limits
- **Don't store sensitive data unencrypted**: Use proper security for persistent storage
- **Don't ignore serialization**: Implement proper serialization for production scenarios
- **Don't forget cleanup**: Implement proper resource disposal for database connections

## 🔧 Troubleshooting

### Issue: Agent doesn't remember previous turns
**Solution**: Ensure you're using a single thread object across all agent runs:
```python
thread = agent.get_new_thread()
await agent.run(prompt1, thread=thread)
await agent.run(prompt2, thread=thread)  # Same thread!
```

### Issue: Facts are duplicated
**Solution**: Implement deduplication in your fact extraction logic (see `UserFactContextProvider._extract_facts()`)

### Issue: Context too long
**Solution**: Limit the number of facts injected using `max_facts` parameter or implement summarization

### Issue: Memory not persisting across sessions
**Solution**: Implement thread serialization or use persistent storage instead of in-memory `FactStore`

## 🚀 Next Steps

After mastering context providers, explore:

1. **[Middleware](../middleware/)** - Add validation, logging, and security
2. **[MCP Integration](../agents/mcp/)** - Connect to external knowledge sources
3. **[Workflows](../workflows/)** - Build multi-agent systems with persistent memory
4. **[Production Patterns](https://learn.microsoft.com/en-us/agent-framework/)** - Deploy agents with robust memory at scale

---

**Ready to build agents with memory?** Start with `1-azure_ai_memory_context_providers.ipynb` and see your agents remember! 🧠
