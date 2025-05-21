import fs from 'fs/promises';
import path from 'path';
// Quitamos las importaciones y usos de fileURLToPath e import.meta.url
// Confiaremos en el __dirname que Babel debería proveer al transpilar a CommonJS para Jest.

export async function loadScripts() {
    // Cuando este archivo ESM es transpilado a CommonJS por Babel para Jest,
    // __dirname (la variable global de CommonJS) debería referirse al directorio
    // del archivo utils/index.js (o su versión transpilada).
    // Si tu estructura es:
    // b-task/
    //   utils/index.js
    //   luas/
    // Entonces, desde b-task/utils/, necesitamos subir un nivel a b-task/ y luego entrar a luas/.
    // Por lo tanto, path.join(__dirname, '../luas') es correcto.
    
    const scriptsDir = path.join(__dirname, '../luas');

    try {
        const scriptFiles = await fs.readdir(scriptsDir);
        const scripts = {};
        for (const file of scriptFiles) {
            if (path.extname(file) === '.lua') {
                const scriptName = path.basename(file, '.lua');
                scripts[scriptName] = await fs.readFile(path.join(scriptsDir, file), 'utf8');
            }
        }
        if (Object.keys(scripts).length === 0) {
            console.warn(`[WARN] utils/index.js: No Lua scripts found in ${scriptsDir}. This might lead to errors in Queue initialization if scripts are expected.`);
        }
        return scripts;
    } catch (err) {
        console.error(`[FATAL] utils/index.js: Failed to load Lua scripts from ${scriptsDir}. Check path, permissions, and if the directory exists. Error: ${err.message}`);
        // Esto es un fallo crítico. Re-lanzar para que la aplicación/tests fallen claramente.
        throw new Error(`Failed to load Lua scripts from ${scriptsDir}: ${err.message}`);
    }
}