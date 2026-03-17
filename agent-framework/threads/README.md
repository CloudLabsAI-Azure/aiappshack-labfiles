# Multi-Turn Conversation Threading Examples

This folder contains a notebook demonstrating **multi-turn conversation management** with **Azure AI agents** using the Agent Framework. Learn how to maintain conversation context, serialize threads, and manage conversation history.

## 🎯 For Azure AI Foundry Users

**Using Azure AI Foundry with `AzureAIAgentClient`? Start here!**

The notebook demonstrates service-managed threads with Azure AI agents - the recommended pattern for production applications.

## 📚 Notebook

| Notebook | Description | Azure AI Compatible |
|----------|-------------|---------------------|
| **[`1-azure-ai-thread-serialization.ipynb`](1-azure-ai-thread-serialization.ipynb)** | **Complete guide** to thread serialization/deserialization with Azure AI agents. Shows conversation persistence, suspend/resume patterns, lightweight serialization (~50 bytes), and cloud-based storage. Perfect for production applications. | ✅ **YES** |

## 🔑 Service-Managed Threads with Azure AI

**How it works:**
- ✅ Conversation history stored in Azure cloud automatically
- ✅ Small serialization payload (~50 bytes - just thread ID)
- ✅ Multi-device sync capability
- ✅ Automatic backup and persistence
- ✅ **Production-ready pattern for Azure AI Foundry**

**Key benefit**: You only save the thread ID, Azure handles all the message storage for you!

## 🔗 Documentation

- 📖 [Multi-Turn Conversation Guide](https://learn.microsoft.com/en-us/agent-framework/user-guide/agents/multi-turn-conversation?pivots=programming-language-python)
- 📖 [Agent Framework Overview](https://learn.microsoft.com/en-us/agent-framework/overview/agent-framework-overview/)
- 📖 [Azure AI Foundry Documentation](https://learn.microsoft.com/en-us/azure/ai-services/)

## 🚀 Quick Start

```bash
# 1. Authenticate
az login --use-device-code

# 2. Run the Azure AI notebook
jupyter notebook 1-azure-ai-thread-serialization.ipynb
```

## 📓 Additional Notebooks (Azure OpenAI Compatible)

These notebooks were converted from the original Python scripts and enhanced with comprehensive documentation.

| Notebook | Status | Description | Converted From |
|----------|--------|-------------|----------------|
| **[`2-custom_chat_message_store_thread.ipynb`](2-custom_chat_message_store_thread.ipynb)** | ✅ Tested | Custom `ChatMessageStoreProtocol` implementation with Azure OpenAI. Shows how to build custom message storage with in-memory persistence, serialization patterns, and Pydantic model integration. | `custom_chat_message_store_thread.py` |
| **[`3-redis_chat_message_store_thread.ipynb`](3-redis_chat_message_store_thread.ipynb)** | ⚠️ Requires Redis | Redis-based distributed conversation storage. Includes 5 comprehensive examples: basic store usage, session management, conversation persistence, serialization, and message limits. | `redis_chat_message_store_thread.py` |
| **[`4-suspend_resume_thread.ipynb`](4-suspend_resume_thread.ipynb)** | ✅ Tested | Thread suspend/resume patterns for both service-managed (lightweight ~50 bytes) and in-memory threads (full message history). Demonstrates conversation persistence and reconnection strategies. | `suspend_resume_thread.py` |

### 🧪 Testing Summary

| Notebook | All Cells Executed | Last Tested | Notes |
|----------|-------------------|-------------|-------|
| 2-custom_chat_message_store_thread.ipynb | ✅ Pass | October 2025 | All examples working with Azure OpenAI. Fixed Pydantic `arbitrary_types_allowed` configuration. |
| 3-redis_chat_message_store_thread.ipynb | ⚠️ Skipped | October 2025 | Code validated but requires `redis-server` running on localhost:6379. All 5 examples fully documented and code-complete. |
| 4-suspend_resume_thread.ipynb | ✅ Pass | October 2025 | All examples working with Azure OpenAI. Both service-managed and in-memory thread patterns verified. |

### 🔧 Key Technical Fixes Applied

During the conversion and testing process, the following technical issues were resolved:

1. **Pydantic Model Configuration**: Added `model_config = {"arbitrary_types_allowed": True}` to handle `ChatMessage` objects in custom store states
2. **Azure OpenAI Migration**: Updated all notebooks from OpenAI SDK to Azure OpenAI with proper `AsyncAzureOpenAI` client initialization
3. **JSON Serialization**: Fixed thread serialization by using `str()` instead of `json.dumps()` for `ChatMessage` objects
4. **Environment Variables**: Added `OPENAI_CHAT_MODEL_ID` and `REDIS_URL` to agent-framework/.env configuration

### 🚀 Environment Setup for Additional Notebooks

These notebooks use Azure OpenAI. Configure in `agent-framework/.env`:

```bash
# Required for all notebooks
AZURE_OPENAI_ENDPOINT=https://your-resource.openai.azure.com/
AZURE_OPENAI_API_KEY=your-api-key
AZURE_OPENAI_CHAT_DEPLOYMENT_NAME=gpt-4o
OPENAI_CHAT_MODEL_ID=gpt-4o

# Required only for Redis notebook (3-redis_chat_message_store_thread.ipynb)
REDIS_URL=redis://localhost:6379
```

**Installing Redis (for notebook 3 only):**
```powershell
# Windows - Using Chocolatey
choco install redis-64

# Windows - Using WSL
wsl sudo apt-get install redis-server
wsl redis-server

# Verify Redis is running
redis-cli ping  # Should return "PONG"
```

## Original Python Scripts

| File | Description |
|------|-------------|
| [`custom_chat_message_store_thread.py`](custom_chat_message_store_thread.py) | Original implementation of custom `ChatMessageStore` (uses OpenAI client) |
| [`redis_chat_message_store_thread.py`](redis_chat_message_store_thread.py) | Original Redis-based message store examples (uses OpenAI client) |
| [`suspend_resume_thread.py`](suspend_resume_thread.py) | Original suspend/resume examples (uses OpenAI client) |
