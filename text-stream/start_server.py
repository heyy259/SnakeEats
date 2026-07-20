"""Simple script to start the TextStreamSystem for testing."""

import asyncio
import logging
import sys

from text_stream_system.system import TextStreamSystem

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def main():
    """Start the system and keep it running."""
    port = 9876
    if len(sys.argv) > 1:
        port = int(sys.argv[1])
    
    logger.info(f"Starting TextStreamSystem on port {port}...")
    system = await TextStreamSystem.start(port=port)
    
    logger.info(f"System started: id={system.system_id} port={system.port}")
    logger.info("Press Ctrl+C to stop...")
    
    try:
        # Keep running until interrupted
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        await system.shutdown()
        logger.info("System stopped.")


if __name__ == "__main__":
    asyncio.run(main())