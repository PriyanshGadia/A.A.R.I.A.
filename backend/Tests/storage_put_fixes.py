# fix_storage_puts.py
import re

def fix_persona_core():
    with open('persona_core.py', 'r') as f:
        content = f.read()
    
    # Fix missing await in _save_persistent_memory
    content = re.sub(
        r'self\.core\.store\.put\("enhanced_memory_index", "persona_memory", self\.memory_index\)',
        r'await self.core.store.put("enhanced_memory_index", "persona_memory", self.memory_index)',
        content
    )
    
    # Fix missing await in store_memory
    content = re.sub(
        r'self\.core\.store\.put\(memory_id, "memory", memory_record\)',
        r'await self.core.store.put(memory_id, "memory", memory_record)',
        content
    )
    
    # Fix close method to await async calls
    content = re.sub(
        r'self\._save_persistent_memory\(\)\s*\n\s*self\._persist_critical_data\(\)',
        r'await self._save_persistent_memory()\n        await self._persist_critical_data()',
        content
    )
    
    with open('persona_core.py', 'w') as f:
        f.write(content)
    print("Fixed persona_core.py")

if __name__ == "__main__":
    fix_persona_core()