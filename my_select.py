from sqlalchemy import func, desc, select, and_

from conf.models import Grade, Teacher, Student, Group, Subject
from conf.db import session


def select_01():
    """
    --Знайти 5 студентів із найбільшим середнім балом з усіх предметів.

    SELECT
        s.id,
        s.fullname AS student_name,
        ROUND(AVG(g.grade), 2) AS average_grade
    FROM students s
    JOIN grades g ON s.id = g.student_id
    GROUP BY s.id
    ORDER BY average_grade DESC
    LIMIT 5;
    :return:
    """
    result = session.query(Student.id, Student.fullname.label("student_name"), func.round(func.avg(Grade.grade), 2).label("average_grade"))\
            .select_from(Student).join(Grade).group_by(Student.id).order_by(desc("average_grade")).limit(5).all()
    return result


def select_02():
    """
    --Знайти студента із найвищим середнім балом з певного предмета.

    SELECT
        s.id,
        s.fullname AS student_name,
        ROUND(AVG(g.grade), 2) AS average_grade
    FROM grades g
    JOIN students s ON s.id = g.student_id
    WHERE g.student_id = 1
    GROUP BY s.id
    ORDER BY average_grade DESC
    LIMIT 1;
    :return:
    """
    result = session.query(Student.id, Student.fullname.label("student_name"), func.round(func.avg(Grade.grade), 2).label("average_grade"))\
            .select_from(Grade).join(Student).filter(Grade.student_id==1).group_by(Student.id).order_by(desc("average_grade")).limit(1).all()
    return result


def select_03(subject_id):
    """
    --Знайти середній бал у групах з певного предмета.

    SELECT
        g.name AS group_name,
        ROUND(AVG(gr.grade), 2) AS average_grade
    FROM grades gr
    JOIN students s ON s.id = gr.student_id
    JOIN groups g ON g.id = s.group_id
    WHERE gr.subject_id = ?
    GROUP BY g.id
    ORDER BY average_grade DESC;
    :return:
    """
    result = session.query(Group.name.label("group_name"), func.round(func.avg(Grade.grade), 2).label("average_grade"))\
            .select_from(Grade).join(Student).join(Group).filter(Grade.subject_id==subject_id).group_by(Group.id).order_by(desc("average_grade")).all()
    return result


def select_04():
    """
    --Знайти середній бал на потоці (по всій таблиці оцінок).

    SELECT
        ROUND(AVG(grade), 2) AS average_grade
    FROM grades;
    :return:
    """
    result = session.query(func.round(func.avg(Grade.grade), 2).label("average_grade"))\
            .select_from(Grade).all()
    return result


def select_05(teacher_id):
    """
    --Знайти які курси читає певний викладач.

    SELECT
        sub.name AS subject_name
    FROM subjects sub
    JOIN teachers t ON sub.teacher_id = t.id
    WHERE t.id = ?;
    :return:
    """
    result = session.query(Subject.name.label("subject_name")).select_from(Subject).join(Teacher).filter(Teacher.id==teacher_id).all()
    return result


def select_06(group_id):
    """
    --Знайти список студентів у певній групі.

    SELECT
        s.fullname AS student_name
    FROM students s
    JOIN groups g ON s.group_id = g.id
    WHERE g.id = ?
    :return:
    """
    result = session.query(Student.fullname.label("student_name")).select_from(Student).join(Group).filter(Group.id==group_id).all()
    return result


def select_07(subject_id, group_id):
    """
    --Знайти оцінки студентів у окремій групі з певного предмета.

    SELECT
        s.group_id,
        s.fullname AS student_name,
        g.grade
    FROM grades g
    JOIN students s ON g.student_id = s.id
    JOIN subjects sub ON g.subject_id = sub.id
    WHERE sub.id = ? AND s.group_id = ?
    ORDER BY s.fullname
    :return:
    """
    result = session.query(Student.group_id, Student.fullname.label("student_name"), Grade.grade).select_from(Grade)\
    .join(Student).join(Subject).filter(Subject.id==subject_id, Student.group_id==group_id).order_by(Student.fullname).all()
    return result


def select_08(teacher_id):
    """
    --Знайти середній бал, який ставить певний викладач зі своїх предметів.

    SELECT
        t.fullname AS teacher_name,
        ROUND(AVG(g.grade), 2) AS average_grade
    FROM grades g
    JOIN subjects sub ON g.subject_id = sub.id
    JOIN teachers t ON sub.teacher_id = t.id
    WHERE t.id = ?
    GROUP BY t.id, t.fullname
    ORDER BY average_grade DESC;
    :return:
    """
    result = session.query(Teacher.fullname.label("teacher_name"), func.round(func.avg(Grade.grade), 2).label("average_grade"))\
    .select_from(Grade).join(Subject).join(Teacher).filter(Teacher.id==teacher_id).group_by(Teacher.id, Teacher.fullname)\
        .order_by(desc("average_grade")).all()
    return result


def select_09(student_id):
    """
    --Знайти список курсів, які відвідує студент.

    SELECT
        sub.name AS course_name
    FROM subjects sub
    JOIN grades g ON sub.id = g.subject_id
    WHERE g.student_id = ?
    GROUP BY sub.name
    ORDER BY sub.name
    :return:
    """
    result = session.query(Subject.name.label("course_name")).select_from(Subject).join(Grade).filter(Grade.student_id==student_id)\
    .group_by(Subject.name).order_by(Subject.name).all()
    return result


def select_10(student_id, teacher_id):
    """
    --Список курсів, які певному студенту читає певний викладач.

    SELECT
        sub.name AS course_name
    FROM subjects sub
    JOIN grades g ON sub.id = g.subject_id
    JOIN teachers t ON sub.teacher_id = t.id
    WHERE g.student_id = ? AND t.id = ?
    GROUP BY sub.name
    ORDER BY sub.name
    :return:
    """
    result = session.query(Subject.name.label("course_name")).select_from(Subject).join(Grade).join(Teacher)\
    .filter(Grade.student_id==student_id, Teacher.id==teacher_id).group_by(Subject.name).order_by(Subject.name).all()
    return result


def select_11(teacher_id, student_id):
    """
    --Середній бал, який певний викладач ставить певному студентові.

    SELECT
        t.fullname AS teacher_name,
        s.fullname AS student_name,
        ROUND(AVG(g.grade), 2) AS average_grade
    FROM grades g
    JOIN students s ON g.student_id = s.id
    JOIN subjects sub ON g.subject_id = sub.id
    JOIN teachers t ON sub.teacher_id = t.id
    WHERE t.id = ? AND s.id = ?
    GROUP BY t.id, t.fullname, s.id, s.fullname
    ORDER BY average_grade DESC
    """
    result = session.query(Teacher.fullname.label("teacher_name"), Student.fullname.label("student_name"), func.round(func.avg(Grade.grade), 2).label("average_grade"))\
    .select_from(Grade).join(Student).join(Subject).join(Teacher).filter(Teacher.id==teacher_id, Student.id==student_id)\
        .group_by(Teacher.id, Teacher.fullname, Student.id, Student.fullname).order_by(desc("average_grade")).all()
    return result


def select_12():
    """
    --Оцінки студентів у певній групі з певного предмета на останньому занятті.

    SELECT MAX(grade_date)
    from grades g
    join students s on s.id =g.student_id
    where g.subject_id =1 and s.id = 1;

    select s.id , s.fullname , g.grade , g.grade_date
    from grades g
    join students s on g.student_id =s.id
    where g.subject_id =1 and s.group_id = 1 and g.grade_date = (
        SELECT MAX(grade_date)
        from grades g
        join students s on s.id =g.student_id
        where g.subject_id =1 and s.group_id = 1
    )
    :return:
    """
    subquery = (select(func.max(Grade.grade_date)).join(Student).filter(and_(
        Grade.subject_id == 1, Student.group_id == 1
    ))).scalar_subquery()

    result = session.query(Student.id, Student.fullname, Grade.grade, Grade.grade_date)\
            .select_from(Grade)\
        .join(Student).filter(Grade.subject_id==1, Student.group_id==1, Grade.grade_date == subquery).all()
    return result


if __name__ == '__main__':
    print(select_01())
    print(select_02())
    print(select_03(4))
    print(select_04())
    print(select_05(1))
    print(select_06(2))
    print(select_07(2, 3))
    print(select_08(4))
    print(select_09(7))
    print(select_10(10, 4))
    print(select_11(3, 12))
    print(select_12())













