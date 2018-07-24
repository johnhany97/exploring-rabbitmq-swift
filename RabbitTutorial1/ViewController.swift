//
//  ViewController.swift
//  RabbitTutorial1
//
//  Created by John Ayad on 24/07/2018.
//  Copyright © 2018 John Ayad. All rights reserved.
//

import UIKit
import RMQClient

class ViewController: UIViewController {

    override func viewDidLoad() {
        super.viewDidLoad()
        // Do any additional setup after loading the view, typically from a nib.
        self.send()
        self.receive()
    }

    override func didReceiveMemoryWarning() {
        super.didReceiveMemoryWarning()
        // Dispose of any resources that can be recreated.
    }
    
    func send() {
        print("Attempting to connect to local RabbitMQ broker")
        let conn = RMQConnection(delegate: RMQConnectionDelegateLogger())
        conn.start()
        let ch = conn.createChannel()
        let q = ch.queue("hello")
        ch.defaultExchange().publish("Hello World!".data(using: .utf8), routingKey: q.name)
        print("Sent 'Hello World!'")
        conn.close()
    }
    
    func receive() {
        print("Attempting to connect to local RabbitMQ broker")
        let conn = RMQConnection(delegate: RMQConnectionDelegateLogger())
        conn.start()
        let ch = conn.createChannel()
        let q = ch.queue("hello")
        print("Waiting for messages.")
        q.subscribe({(_ message: RMQMessage) -> Void in
            print("Received \(String(data: message.body, encoding: String.Encoding.utf8)!)")
        })
    }

}

