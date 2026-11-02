module.exports = {
  testEnvironment: 'node',
  roots: ['<rootDir>/test'],
  testMatch: ['**/*.test.ts'],
  transform: { '^.+\\.tsx?$': 'ts-jest' },
  // Synthesizing a stack is not instant, and several suites do it.
  testTimeout: 30000,
};
