import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

/**
 * Carga los scripts Lua desde el directorio scripts
 * @returns {Promise<Object>} Objeto con los scripts cargados
 */
export async function loadScripts() {
    // scriptsDir ahora apuntará correctamente a la carpeta 'luas' dentro de tu paquete
    const scriptsDir = path.join(__dirname, '../luas');
    const scriptFiles = await fs.readdir(scriptsDir);
    const scripts = {};

    for (const file of scriptFiles) {
        if (path.extname(file) === '.lua') {
            const scriptName = path.basename(file, '.lua');
            scripts[scriptName] = await fs.readFile(path.join(scriptsDir, file), 'utf8');
        }
    }
    return scripts;
}