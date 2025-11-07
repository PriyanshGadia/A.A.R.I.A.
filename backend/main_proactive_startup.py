# main_proactive_startup.py
import asyncio
import logging
import time
from interaction_core import create_interaction_core  # your existing module
from proactive_comm import ProactiveCommunicator
from console_adapter import ConsoleAdapter

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("main_proactive_startup")


# --- Minimal stub persona/cognition/autonomy for demo --- #
class PersonaStub:
    core = None
    async def notify(self, envelope):
        print("PersonaStub.notify called:", envelope)

class CognitionStub: pass
class AutonomyStub: pass

# --- Demo startup --- #
async def main():
    # Create persona/cognition/autonomy objects. Replace with your real components
    persona = PersonaStub()
    cognition = CognitionStub()
    autonomy = AutonomyStub()

    # Create InteractionCore (do NOT auto-start it inside factory)
    interaction = await create_interaction_core(persona, cognition, autonomy, config={})
    # wire a deliver_hook for interaction so that Proactive can call interaction.send_outbound
    async def deliver_via_interaction(envelope):
        # convert envelope into OutboundMessage-like dict for interaction
        try:
            await interaction.send_outbound(
                type("M", (), {})()  # dummy object; better to call interaction.send_outbound with OutboundMessage instance
            )
        except Exception:
            # fallback: call interaction.send_outbound with actual OutboundMessage if available
            from interaction_core import OutboundMessage as OB
            msg = OB(channel=envelope.get("channel"), content=envelope.get("payload", {}).get("text", ""), user_id=envelope.get("subject_identity"))
            await interaction.send_outbound(msg)
        return True

    # create ProactiveCommunicator with interaction's core as 'core' so it can use store
    proactive = ProactiveCommunicator(core=interaction, deliver_hook=None)

    # integrate proactively with InteractionCore (so deliver_hook uses interaction.schedule_proactive)
    proactive.integrate_with_interaction(interaction)

    # register simple console adapter (for channel "console")
    console = ConsoleAdapter()
    proactive.register_adapter("console", console)
    proactive.register_adapter("push", console)  # for demo, map push to console too

    # IMPORTANT: start interaction first (will start autosave/outbound/proactive inside InteractionCore)
    await interaction.start()

    # start proactive communicator AFTER interaction.start()
    await proactive.start()

    # Example: ask AARIA to remind you in 10 seconds
    env_id = await proactive.schedule_reminder("This is a test reminder from AARIA", when_text="in 10s", subject_identity="owner_primary", channel="console")
    print("Scheduled proactive envelope id:", env_id)

    # Demonstrate immediate enqueue + scheduling multiple messages
    env2 = await proactive.schedule_reminder("Follow-up: don't forget to check logs", when_ts=time.time() + 15.0, subject_identity="owner_primary", channel="push")
    print("Scheduled second envelope:", env2)

    # Keep the event loop running so proactive worker can run and deliver reminders.
    # In a real server you would integrate this into your application's event loop or ASGI server.
    print("Main sleeping for 25 seconds to allow reminders to deliver...")
    await asyncio.sleep(25)

    # stop proactive and interaction gracefully
    await proactive.stop()
    await interaction.stop()
    print("Shutdown complete")

if __name__ == "__main__":
    asyncio.run(main())