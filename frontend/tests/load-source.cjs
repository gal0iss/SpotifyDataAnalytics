const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { createRequire } = require('node:module');
const ts = require('typescript');

// Transpile the actual frontend in memory, replacing only browser boundaries.
function sourceLoader(mocks = {}) {
  const cache = new Map();
  function load(filename) {
    filename = path.resolve(__dirname, '../src', filename);
    if (cache.has(filename)) return cache.get(filename).exports;
    const module = { exports: {} };
    cache.set(filename, module);
    const nativeRequire = createRequire(filename);
    const requireSource = (specifier) => {
      if (Object.hasOwn(mocks, specifier)) return mocks[specifier];
      if (!specifier.startsWith('.')) return nativeRequire(specifier);
      const base = path.resolve(path.dirname(filename), specifier);
      const resolved = ['.ts', '.tsx', '.js'].map((ext) => base + ext)
        .find((candidate) => fs.existsSync(candidate));
      if (!resolved) throw new Error(`Cannot resolve ${specifier} from ${filename}`);
      return load(resolved);
    };
    const { outputText } = ts.transpileModule(fs.readFileSync(filename, 'utf8'), {
      compilerOptions: {
        target: ts.ScriptTarget.ES2022,
        module: ts.ModuleKind.CommonJS,
        jsx: ts.JsxEmit.ReactJSX,
        esModuleInterop: true,
      },
      fileName: filename,
    });
    vm.runInThisContext(`(function(exports, require, module) {${outputText}\n})`, {
      filename,
    })(module.exports, requireSource, module);
    return module.exports;
  }
  return load;
}

module.exports = { sourceLoader };
