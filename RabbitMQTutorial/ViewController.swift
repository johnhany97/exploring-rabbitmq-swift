//
//  ViewController.swift
//  RabbitMQTutorial
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
        // This is to make sure RabbitMQ will never lose the queue
        let queue = channel.queue("task_queue", options: .durable)
        let msgData = msg.data(using: .utf8)
        // We mark messages as persistent as well so that even if RabbitMQ restarts, the messages will still be there
        // It's not a full guarantee that a message won't be lost. Unfortunately.
        // (Read: https://www.rabbitmq.com/tutorials/tutorial-two-swift.html)
        channel.defaultExchange().publish(msgData, routingKey: queue.name, persistent: true)
        print("Sent \(msg)")
        connection.close()
    }
    
    func workerNamed(_ name: String) {
        let connection = RMQConnection(delegate: RMQConnectionDelegateLogger())
        connection.start()
        let channel = connection.createChannel()
        // This is to make sure RabbitMQ will never lose the queue
        let queue = channel.queue("task_queue", options: .durable)
        // This is to avoid default behaviour and to properly distribute tasks amongst workers
        // This will result in tasks being given to the next empty worker. Also, this means that you won't receive
        // tasks as a worker till you finish the task on hand and acknowledge it
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

