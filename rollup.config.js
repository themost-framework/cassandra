/*eslint-env es6 */
const { babel } = require('@rollup/plugin-babel');
const commonjs = require('@rollup/plugin-commonjs');
const pkg = require('./package.json');
const dts = require('rollup-plugin-dts').default;

const external = Object.keys(pkg.dependencies || {}).concat(Object.keys(pkg.peerDependencies || {}))

module.exports = [
    {
        input: 'src/index.js',
        output: {
            dir: 'dist',
            format: 'cjs',
            sourcemap: true
        },
        external: external,
        plugins: [
            commonjs(),
            babel({ babelHelpers: 'bundled' })
        ]
    },
    {
        input: 'src/index.js',
        output: {
            file: 'dist/index.esm.js',
            format: 'esm',
            sourcemap: true
        },
        external: external,
        plugins: [babel({ babelHelpers: 'bundled' })]
    },
    {
        input: 'src/index.d.ts',
        output: [
            {
                file: 'dist/index.d.ts'
            }
        ],
        external: external,
        plugins: [dts()],
    }
];
