import logging
import random


from faker import Faker
from sqlalchemy.exc import SQLAlchemyError

from conf.db import session
from conf.models import Teacher, Group, Student, Subject, Grade


fake = Faker("uk-UA")

logging.basicConfig(level=logging.ERROR)

def adding_teachers():
    # Додавання викладачів
    teachers = [Teacher(fullname=fake.name()) for _ in range(random.randint(3, 5))]
    session.add_all(teachers)
    return teachers

def adding_groups():
    # Додавання груп
    groups = [Group(name=fake.word()) for _ in range(3)]
    session.add_all(groups)
    return groups

def adding_students(groups):
    # Додавання студентів
    if not groups:
        logging.error("Groups list is empty. Cannot add students.")
        return []
    students = [Student(fullname=fake.name(), group=random.choice(groups)) for _ in range(random.randint(30, 50))]
    session.add_all(students)
    return students

def adding_subjects(teachers):
    # Додавання предметів
    if not teachers:
        logging.error("Teachers list is empty. Cannot add subjects.")
        return []
    subjects = [Subject(name=fake.name(), teacher=random.choice(teachers)) for _ in range(random.randint(5, 8))]
    session.add_all(subjects)
    return subjects

def adding_grades(students, subjects):
    # Додавання оцінок
    if not students or not subjects:
        logging.error("Students or subjects list is empty. Cannot add grades.")
        return
    for student in students:
        for subject in subjects:
            grades = [Grade(student=student,
                            subject=subject,
                            grade=random.randint(0, 100),
                            grade_date=fake.date_this_decade(),)
                      for _ in range(random.randint(5, 20))]
            session.add_all(grades)


if __name__ == '__main__':
    try:
        teachers = adding_teachers()
        groups = adding_groups()
        students = adding_students(groups)
        subjects = adding_subjects(teachers)
        adding_grades(students, subjects)
        session.commit()
    except SQLAlchemyError as e:
        logging.error(f"Database error: {e}")
        session.rollback()
    finally:
        session.close()