/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.kalsym.kafkaentries.controller;

import com.kalsym.kafkaentries.service.Service;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 *
 * @author hasan
 */
@RestController
@RequestMapping("/kafka/entries/")
public class Controller {
    @Autowired Service service;
    
    @GetMapping("/insert")
    public String insertEntries() throws InterruptedException{
        return service.insertValues();
    }
    @GetMapping("/fetch")
    public List<String> fetchEntries() throws InterruptedException{
        return service.fetchValues();
    }
}
