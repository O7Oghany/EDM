package net.shirazy.paymentservice.controller;

import jakarta.persistence.EntityNotFoundException;
import java.util.List;
import net.shirazy.paymentservice.model.Payment;
import net.shirazy.paymentservice.model.PaymentRequest;
import net.shirazy.paymentservice.service.PaymentService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/payment")
public class PaymentController {

  @Autowired
  private PaymentService paymentService;

  @PostMapping
  public ResponseEntity<String> makePayment(
    @RequestBody PaymentRequest request
  ) {
    paymentService.processPayment(request);
    return new ResponseEntity<>("Payment processed", HttpStatus.OK);
  }

  @GetMapping
  public ResponseEntity<List<Payment>> getAllPayments() {
    return new ResponseEntity<>(
      paymentService.findAllPayments(),
      HttpStatus.OK
    );
  }

  @GetMapping("/{id}")
  public ResponseEntity<Payment> getPaymentsById(@PathVariable("id") Long id) {
    return new ResponseEntity<>(
      paymentService.findPaymentById(id),
      HttpStatus.OK
    );
  }

  @GetMapping("/user/{id}")
  public ResponseEntity<List<Payment>> getPaymentsByUserId(
    @PathVariable("id") Long userId
  ) {
    return new ResponseEntity<>(
      paymentService.findPaymentsByUserId(userId),
      HttpStatus.OK
    );
  }

  @PutMapping("/{id}")
  public ResponseEntity<Payment> updatePayment(
    @PathVariable("id") Long id,
    @RequestBody Payment updatedPayment
  ) {
    if (paymentService.findPaymentById(id) == null) {
      throw new EntityNotFoundException("Payment not found");
    }
    updatedPayment.setId(id);
    Payment savedPayment = paymentService.updatePayment(updatedPayment);
    return new ResponseEntity<>(savedPayment, HttpStatus.OK);
  }

  @DeleteMapping("/{id}")
  public ResponseEntity<String> deletePayment(@PathVariable("id") Long id) {
    Payment payment = paymentService.findPaymentById(id);
    if (payment == null) {
      return new ResponseEntity<>("Payment not found", HttpStatus.NOT_FOUND);
    }
    paymentService.deletePayment(id);
    return new ResponseEntity<>("Payment deleted", HttpStatus.OK);
  }
}
