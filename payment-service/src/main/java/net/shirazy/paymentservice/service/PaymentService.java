package net.shirazy.paymentservice.service;

import ch.qos.logback.classic.Logger;
import jakarta.persistence.EntityNotFoundException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import net.shirazy.paymentservice.avro.PaymentEvent;
import net.shirazy.paymentservice.enums.PaymentStatus;
import net.shirazy.paymentservice.model.Payment;
import net.shirazy.paymentservice.model.PaymentRequest;
import net.shirazy.paymentservice.repository.PaymentRepository;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class PaymentService {

  private static final Logger logger = (Logger) LoggerFactory.getLogger(
    PaymentService.class
  );

  @Autowired
  private KafkaTemplate<String, byte[]> byteKafkaTemplate;

  @Autowired
  private PaymentRepository paymentRepository;

  public void processPayment(PaymentRequest request) {
    Payment payment = new Payment();
    payment.setUserId(request.getUserId());
    payment.setAmount(request.getAmount());
    payment.setStatus(PaymentStatus.PENDING);

    String timestamp = LocalDateTime
      .now()
      .format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
    LocalDateTime dateTime = LocalDateTime.parse(timestamp, formatter);
    payment.setTransactionId(request.getUserId() + "-" + timestamp);
    payment.setTimestamp(dateTime);

    paymentRepository.save(payment);

    publishPaymentEvent(
      request.getUserId(),
      request.getAmount(),
      PaymentStatus.COMPLETED,
      "payment_created_topic"
    );
    payment.setStatus(PaymentStatus.COMPLETED);
    paymentRepository.save(payment);
  }

  public List<Payment> findAllPayments() {
    return paymentRepository.findAll();
  }

  public Payment findPaymentById(Long id) {
    return paymentRepository
      .findById(id)
      .orElseThrow(() -> new EntityNotFoundException("Payment not found"));
  }

  public List<Payment> findPaymentsByUserId(Long userId) {
    return paymentRepository.findByUserId(userId);
  }

  public Payment updatePayment(Payment updatedPayment) {
    Payment existingPayment = paymentRepository
      .findById(updatedPayment.getId())
      .orElseThrow(() -> new EntityNotFoundException("Payment not found"));
    Field[] fields = Payment.class.getDeclaredFields();
    for (Field field : fields) {
      try {
        field.setAccessible(true);
        Object newValue = field.get(updatedPayment);
        if (newValue != null) {
          field.set(existingPayment, newValue);
        }
      } catch (IllegalAccessException e) {
        e.printStackTrace();
        logger.error("Error while updating payment", e);
      }
    }
    logger.info("user ID: {}", existingPayment.getUserId());
    Payment savedPayment = paymentRepository.save(existingPayment);

    publishPaymentEvent(
      existingPayment.getUserId(),
      existingPayment.getAmount(),
      PaymentStatus.UPDATED,
      "payment_updated_topic"
    );

    return savedPayment;
  }

  public void deletePayment(Long id) {
    Payment existingPayment = paymentRepository
      .findById(id)
      .orElseThrow(() -> new EntityNotFoundException("Payment not found"));

    publishPaymentEvent(
      existingPayment.getUserId(),
      existingPayment.getAmount(),
      PaymentStatus.DELETED,
      "payment_deleted_topic"
    );
    paymentRepository.deleteById(id);
  }

  private void publishPaymentEvent(
    Long userId,
    Double amount,
    PaymentStatus paymentStatus,
    String topicName
  ) {
    PaymentEvent paymentEvent = new PaymentEvent();
    paymentEvent.setUserId(userId.toString());
    paymentEvent.setAmount(amount);
    String timestamp = LocalDateTime
      .now()
      .format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
    paymentEvent.setTransactionId(userId + "-" + timestamp);
    paymentEvent.setTimestamp(timestamp);
    paymentEvent.setStatus(paymentStatus.toString());

    try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
      SpecificDatumWriter<PaymentEvent> writer = new SpecificDatumWriter<>(
        PaymentEvent.class
      );
      writer.write(paymentEvent, encoder);
      encoder.flush();
      byteKafkaTemplate.send(topicName, out.toByteArray());
    } catch (IOException e) {
      logger.error("Error while serializing PaymentEvent", e);
      throw new RuntimeException("Error while serializing PaymentEvent", e);
    }
  }
}
