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
        self.workerNamed("Jack")
        self.workerNamed("Jill")
        sleep(1)
        self.newTask("Hello World...")
        self.newTask("Just one this time.")
        self.newTask("None")
        self.newTask("Two..dots")
    }

    override func didReceiveMemoryWarning() {
        super.didReceiveMemoryWarning()
        // Dispose of any resources that can be recreated.
    }
    
    func newTask(_ msg: String) {
        let connection = RMQConnection(delegate: RMQConnectionDelegateLogger())
        connection.start()
        let channel = connection.createChannel()
        let queue = channel.queue("task_queue", options: .durable)
        let msgData = msg.data(using: .utf8)
        channel.defaultExchange().publish(msgData, routingKey: queue.name, persistent: true)
        print("Sent \(msg)")
        connection.close()
    }
    
    func workerNamed(_ name: String) {
        let connection = RMQConnection(delegate: RMQConnectionDelegateLogger())
        connection.start()
        let channel = connection.createChannel()
        let queue = channel.queue("task_queue", options: .durable)
        channel.basicQos(1, global:false)
        print("\(name): Waiting for messages")
        let manualAck = RMQBasicConsumeOptions()
        queue.subscribe(manualAck, handler: {(_ message: RMQMessage) -> Void in
            let messageText = String(data: message.body, encoding: .utf8)
            print("\(name): Received \(messageText!)")
            // imitate some work
            let sleepTime = UInt32(messageText!.components(separatedBy:".").count) - 1
            print("\(name): Sleeping for \(sleepTime) seconds")
            sleep(sleepTime)
            channel.ack(message.deliveryTag)
        })
    }
}

