import { promises as fs } from 'fs';
import path from 'path';

/**
 * Carga los scripts Lua desde el directorio scripts
 * @returns {Promise<Object>} Objeto con los scripts cargados
 */
export async function loadScripts() {
    const scriptsDir = path.join(process.cwd(), 'scripts');
    const scriptFiles = {
        addJob: 'addJob.lua',
        getNextJob: 'getNextJob.lua',
        completeJob: 'completeJob.lua',
        drain: 'drain.lua'
    };

    const scripts = {};
    for (const [name, file] of Object.entries(scriptFiles)) {
        const scriptPath = path.join(scriptsDir, file);
        const script = await fs.readFile(scriptPath, 'utf8');
        scripts[name] = script;
    }

    return scripts;
} 