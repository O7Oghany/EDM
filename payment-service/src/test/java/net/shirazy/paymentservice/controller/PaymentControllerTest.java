package net.shirazy.paymentservice.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import net.shirazy.paymentservice.model.Payment;
import net.shirazy.paymentservice.model.PaymentRequest;
import net.shirazy.paymentservice.repository.PaymentRepository;
import net.shirazy.paymentservice.service.PaymentService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

@ExtendWith(MockitoExtension.class)
public class PaymentControllerTest {

  private PaymentRequest paymentRequest;
  private final Long id = 1L;
  private final Long userId = 1L;
  private final Double amount = 100.0;
  private Payment payment;

  @InjectMocks
  private PaymentController paymentController;

  @Mock
  private PaymentService paymentService;

  @Mock
  private PaymentRepository paymentRepository;

  @BeforeEach
  void setUp() {
    paymentRequest = new PaymentRequest();
    payment = new Payment();
    payment.setId(id);
    payment.setUserId(userId);
    payment.setAmount(amount);
  }

  @Test
  void testDeletePayment() {
    // Act: Call the method to be tested
    when(paymentService.findPaymentById(id)).thenReturn(payment);
    ResponseEntity<String> responseEntity = paymentController.deletePayment(id);

    // Assert: Verify the expected behavior
    verify(paymentService, times(1)).findPaymentById(id); // Ensure the service method was called
    verify(paymentService, times(1)).deletePayment(id); // Ensure the service delete method was called

    // Check the response status code and message for "OK"
    assertSame(HttpStatus.OK, responseEntity.getStatusCode());
    assertEquals("Payment deleted", responseEntity.getBody()); // Customize this based on your actual response message
  }

  @Test
  void testGetAllPayments() {
    when(paymentService.findAllPayments())
      .thenReturn(Collections.singletonList(payment));
    ResponseEntity<List<Payment>> responseEntity = paymentController.getAllPayments();

    verify(paymentService, times(1)).findAllPayments();

    assertSame(HttpStatus.OK, responseEntity.getStatusCode());
    assertNotNull(responseEntity.getBody());

    assertEquals(1, responseEntity.getBody().size());
    Payment found = responseEntity.getBody().get(0);
    assertEquals(id, found.getId());
    assertEquals(userId, found.getUserId());
    assertEquals(amount, found.getAmount(), 0.001);
  }

  @Test
  void testGetPaymentsById() {
    when(paymentService.findPaymentById(id)).thenReturn(payment);
    ResponseEntity<Payment> responseEntity = paymentController.getPaymentsById(
      id
    );

    verify(paymentService, times(1)).findPaymentById(id);

    assertSame(HttpStatus.OK, responseEntity.getStatusCode());
    assertNotNull(responseEntity.getBody());
    Payment found = responseEntity.getBody();

    assertEquals(id, found.getId());
    assertEquals(1L, found.getUserId());
    assertEquals(100.0, found.getAmount());
  }

  @Test
  void testGetPaymentsByUserId() {
    when(paymentService.findPaymentsByUserId(userId))
      .thenReturn(Collections.singletonList(payment));
    ResponseEntity<List<Payment>> responseEntity = paymentController.getPaymentsByUserId(
      userId
    );

    verify(paymentService, times(1)).findPaymentsByUserId(userId);

    assertSame(HttpStatus.OK, responseEntity.getStatusCode());
    assertNotNull(responseEntity.getBody());

    assertEquals(1, responseEntity.getBody().size());
    Payment found = responseEntity.getBody().get(0);
    assertEquals(id, found.getId());
    assertEquals(userId, found.getUserId());
    assertEquals(amount, found.getAmount(), 0.001);
  }

  @Test
  void testMakePayment() {
    ResponseEntity<String> responseEntity = paymentController.makePayment(
      paymentRequest
    );

    verify(paymentService, times(1)).processPayment(paymentRequest);

    assertSame(HttpStatus.OK, responseEntity.getStatusCode());
  }

  @Test
  void testUpdatePayment() {
    // Arrange
    Payment updatedPayment = new Payment();
    updatedPayment.setId(id);
    updatedPayment.setUserId(userId);
    updatedPayment.setAmount(200.0);

    when(paymentService.findPaymentById(id)).thenReturn(updatedPayment);
    when(paymentService.updatePayment(any(Payment.class)))
      .thenReturn(updatedPayment);

    // Act
    ResponseEntity<Payment> responseEntity = paymentController.updatePayment(
      id,
      updatedPayment
    );

    // Verify
    ArgumentCaptor<Payment> paymentCaptor = ArgumentCaptor.forClass(
      Payment.class
    );
    verify(paymentService, times(1)).updatePayment(paymentCaptor.capture());

    // Assert
    assertSame(HttpStatus.OK, responseEntity.getStatusCode());
    assertNotNull(responseEntity.getBody());

    Payment responseBody = responseEntity.getBody();
    assertNotNull(responseBody.getId());
    assertEquals(id, responseBody.getId());
    assertEquals(200.0, responseBody.getAmount(), 0.001);
    assertEquals(userId, responseBody.getUserId());
  }

  @AfterEach
  void tearDown() {
    reset(paymentService);
    reset(paymentRepository);
  }
}
