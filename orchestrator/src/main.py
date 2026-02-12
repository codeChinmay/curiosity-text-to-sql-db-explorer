import sys
import asyncio
from dotenv import load_dotenv

load_dotenv()

try:
    from src.agent import Agent
except ImportError:
    # Fallback if run directly from src/ directory or similar context
    from agent import Agent

async def main():
    print("Orchestrator Service Initialized")
    if len(sys.argv) > 1:
        query = " ".join(sys.argv[1:])
        print(f"Received query: {query}")
        
        agent = Agent()
        try:
            result = await agent.run(query)
            print("Final Result:")
            print(result)
        except Exception as e:
            print(f"Error during execution: {e}")
            import traceback
            traceback.print_exc()
    else:
        print("Usage: python -m src.main <query>")

if __name__ == "__main__":
    asyncio.run(main())
