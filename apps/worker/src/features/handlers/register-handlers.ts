import { registerHandler } from './handler.registry.js';
import { sendEmailHandler } from './send-email.handler.js';
import { crashTestHandler } from './crash-test.handler.js';

export function registerDefaultHandlers(): void {
  registerHandler('send_email', sendEmailHandler);
  registerHandler('crash_test', crashTestHandler);

  console.log('Registered job handlers:', ['send_email', 'crash_test']);
}
