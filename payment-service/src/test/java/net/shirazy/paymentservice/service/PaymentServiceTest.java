package net.shirazy.paymentservice.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import net.shirazy.paymentservice.avro.PaymentEvent;
import net.shirazy.paymentservice.model.Payment;
import net.shirazy.paymentservice.model.PaymentRequest;
import net.shirazy.paymentservice.repository.PaymentRepository;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

@ExtendWith(MockitoExtension.class)
public class PaymentServiceTest {

  private final Long id = 1L;
  private final Long userId = 1L;
  private final Double amount = 100.0;
  private Payment payment;

  @InjectMocks
  private PaymentService paymentService;

  @Mock
  private PaymentRepository paymentRepository;

  @Mock
  private KafkaTemplate<String, byte[]> byteKafkaTemplate;

  @Captor
  ArgumentCaptor<String> topicCaptor;

  @Captor
  ArgumentCaptor<String> payloadCaptor;

  @Captor
  private ArgumentCaptor<byte[]> bytePayloadCaptor;

  @BeforeEach
  void setup() {
    payment = new Payment();
    payment.setId(id);
    payment.setUserId(userId);
    payment.setAmount(amount);
    when(paymentRepository.existsById(id)).thenReturn(true);
    when(paymentRepository.findAll())
      .thenReturn(Collections.singletonList(payment));
    when(paymentRepository.findById(id)).thenReturn(Optional.of(payment));
    when(paymentRepository.findByUserId(userId))
      .thenReturn(Collections.singletonList(payment));
  }

  @Test
  void testDeletePayment() {
    paymentService.deletePayment(id);
    verify(paymentRepository, times(1)).deleteById(id);

    verify(byteKafkaTemplate, times(1))
      .send(topicCaptor.capture(), bytePayloadCaptor.capture());

    assertEquals("payment_deleted_topic", topicCaptor.getValue());

    byte[] capturedPayload = bytePayloadCaptor.getValue();
    DatumReader<PaymentEvent> reader = new SpecificDatumReader<>(
      PaymentEvent.class
    );
    Decoder decoder = DecoderFactory.get().binaryDecoder(capturedPayload, null);
    PaymentEvent decodedPaymentEvent;
    try {
      decodedPaymentEvent = reader.read(null, decoder);
      assertEquals(
        Long.toString(payment.getUserId()),
        decodedPaymentEvent.getUserId().toString().trim()
      );
      assertEquals(payment.getAmount(), decodedPaymentEvent.getAmount());
    } catch (IOException e) {
      System.out.println("Actual Type: " + e.getMessage());
    }
  }

  @Test
  void testFindAllPayments() {
    Payment found = paymentService.findAllPayments().get(0);

    assertNotNull(found);
    assertEquals(id, found.getId());
    assertEquals(1L, found.getUserId());
    assertEquals(100.0, found.getAmount());
  }

  @Test
  void testFindPaymentById() {
    Payment found = paymentService.findPaymentById(id);

    assertNotNull(found);
    assertEquals(id, found.getId());
    assertEquals(1L, found.getUserId());
    assertEquals(100.0, found.getAmount());
  }

  @Test
  void testFindPaymentsByUserId() {
    List<Payment> found = paymentService.findPaymentsByUserId(userId);

    assertNotNull(found);
    assertEquals(1, found.size());
    assertEquals(id, found.get(0).getId());
    assertEquals(1L, found.get(0).getUserId());
    assertEquals(100.0, found.get(0).getAmount());
  }

  @Test
  void testProcessPayment() {
    PaymentRequest paymentRequest = new PaymentRequest();
    paymentRequest.setUserId(userId);
    paymentRequest.setAmount(amount);
    paymentService.processPayment(paymentRequest);

    ArgumentCaptor<Payment> paymentCaptor = ArgumentCaptor.forClass(
      Payment.class
    );
    verify(paymentRepository, times(1)).save(paymentCaptor.capture());
    Payment savedPayment = paymentCaptor.getValue();
    assertNotNull(savedPayment);
    assertEquals(userId, savedPayment.getUserId());
    assertEquals(amount, savedPayment.getAmount());

    verify(byteKafkaTemplate, times(1))
      .send(topicCaptor.capture(), bytePayloadCaptor.capture());

    byte[] avroBytes = bytePayloadCaptor.getValue();
    DatumReader<PaymentEvent> reader = new SpecificDatumReader<>(
      PaymentEvent.class
    );
    Decoder decoder = DecoderFactory.get().binaryDecoder(avroBytes, null);
    PaymentEvent decodedPaymentEvent;
    try {
      decodedPaymentEvent = reader.read(null, decoder);
      assertEquals("payment_created_topic", topicCaptor.getValue());
      assertEquals(
        Long.toString(paymentRequest.getUserId()),
        decodedPaymentEvent.getUserId().toString().trim()
      );
    } catch (IOException e) {
      System.out.println("Actual Type: " + e.getMessage());
    }
  }

  @Test
  void testUpdatePayment() {
    Payment updatedPayment = new Payment();
    updatedPayment.setId(id);
    updatedPayment.setUserId(userId);
    updatedPayment.setAmount(200.0);

    when(paymentRepository.save(any(Payment.class))).thenReturn(updatedPayment);

    Payment result = paymentService.updatePayment(updatedPayment);

    // Verify and Assert
    ArgumentCaptor<Payment> paymentCaptor = ArgumentCaptor.forClass(
      Payment.class
    );
    verify(paymentRepository, times(1)).save(paymentCaptor.capture());

    assertNotNull(result);
    assertEquals(id, result.getId());
    assertEquals(200.0, result.getAmount());
    assertEquals(result, updatedPayment);

    verify(byteKafkaTemplate, times(1))
      .send(topicCaptor.capture(), bytePayloadCaptor.capture());
    assertEquals("payment_updated_topic", topicCaptor.getValue());
    byte[] capturedPayload = bytePayloadCaptor.getValue();
    DatumReader<PaymentEvent> reader = new SpecificDatumReader<>(
      PaymentEvent.class
    );
    Decoder decoder = DecoderFactory.get().binaryDecoder(capturedPayload, null);
    PaymentEvent decodedPaymentEvent;
    try {
      decodedPaymentEvent = reader.read(null, decoder);
      assertEquals(
        Long.toString(updatedPayment.getUserId()),
        decodedPaymentEvent.getUserId().toString().trim()
      );
      assertEquals(updatedPayment.getAmount(), decodedPaymentEvent.getAmount());
    } catch (IOException e) {
      System.out.println("Actual Type: " + e.getMessage());
    }
  }

  @AfterEach
  void tearDown() {
    reset(paymentRepository);
  }
}
