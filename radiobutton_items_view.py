from PyQt5.QtWidgets import QDialog, QPushButton, QButtonGroup, QRadioButton, QVBoxLayout, QScrollArea, QWidget, \
    QGridLayout


class RadiobuttonItemsView(QDialog):
    def __init__(self, dbs, selected_db):
        super().__init__()
        self.layout = QGridLayout()
        self.selected_db = selected_db
        self.dbs = dbs

        btn_ok = QPushButton('OK', self)
        btn_ok.clicked.connect(self.ok_pressed)
        btn_cancel = QPushButton('Cancel', self)
        btn_cancel.clicked.connect(self.cancel_pressed)

        scroll = QScrollArea(self)
        scroll.setWidgetResizable(True)
        self.content = QWidget()
        scroll.setWidget(self.content)

        rb_vbox = QVBoxLayout(self.content)
        rb_vbox.addStretch(1)
        self.button_group = QButtonGroup()

        # btn_vbox = QVBoxLayout()
        # btn_vbox.addStretch(1)
        # btn_vbox.addWidget(btn_ok)
        # btn_vbox.addWidget(btn_cancel)

        for db in self.dbs:
            self.button_name = QRadioButton(f"{db}")
            self.button_name.setObjectName(f"radiobtn_{db}")
            rb_vbox.addWidget(self.button_name)
            self.button_group.addButton(self.button_name)

        self.layout.addWidget(scroll, 0, 0)
        self.layout.addWidget(btn_ok, 0, 1)
        self.layout.addWidget(btn_cancel, 0, 2)
        self.setStyleSheet("QScrollArea{min-width:300 px; min-height: 400px}")

        self.setLayout(self.layout)
        self.setModal(True)
        self.show()

    def ok_pressed(self):
        self.selected_db = ''
        for x in range(len(self.button_group.buttons())):
            if self.button_group.buttons()[x].isChecked():
                self.selected_db = self.button_group.buttons()[x].text()
        self.close()

    def cancel_pressed(self):
        self.close()
