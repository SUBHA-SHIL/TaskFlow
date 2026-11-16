import { registerHandler } from './handler.registry.js';
import { sendEmailHandler } from './send-email.handler.js';

export function registerDefaultHandlers(): void {
  registerHandler('send_email', sendEmailHandler);

  console.log('Registered job handlers:', [
    'send_email',
  ]);
}
