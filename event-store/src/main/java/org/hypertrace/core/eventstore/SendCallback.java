package org.hypertrace.core.eventstore;

/**
 * A callback interface that allows clients to receive notifications when a send operation
 * completes. This can be used to handle both successful sends and errors asynchronously.
 */
public interface SendCallback {

  /**
   * Called when a send operation completes.
   *
   * @param result The result of the send operation if successful, null if an exception occurred
   * @param exception The exception thrown during the send operation, null if the send was
   *     successful
   */
  void onCompletion(SendResult result, Exception exception);
}

